// Copyright 2012 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "exec/exec-node.h"

#include <sstream>
#include <unistd.h>  // for sleep()

#include <thrift/protocol/TDebugProtocol.h>

#include "codegen/llvm-codegen.h"
#include "common/object-pool.h"
#include "common/status.h"
#include "exprs/expr.h"
#include "exec/aggregation-node.h"
#include "exec/hash-join-node.h"
#include "exec/hdfs-scan-node.h"
#include "exec/hbase-scan-node.h"
#include "exec/exchange-node.h"
#include "exec/merge-node.h"
#include "exec/cross-join-node.h"
#include "exec/topn-node.h"
#include "exec/select-node.h"
#include "exec/pythia-reader-node.h"
#include "exec/shm-scan-node.h"
#include "runtime/descriptors.h"
#include "runtime/mem-tracker.h"
#include "runtime/mem-pool.h"
#include "runtime/row-batch.h"
#include "runtime/runtime-state.h"
#include "util/debug-util.h"
#include "util/runtime-profile.h"

using namespace llvm;
using namespace std;
using namespace boost;

namespace impala {

const string ExecNode::ROW_THROUGHPUT_COUNTER = "RowsReturnedRate";

int ExecNode::GetNodeIdFromProfile(RuntimeProfile* p) {
  return p->metadata();
}

ExecNode::RowBatchQueue::RowBatchQueue(int max_batches) :
    BlockingQueue<RowBatch*>(max_batches) {
}

ExecNode::RowBatchQueue::~RowBatchQueue() {
  DCHECK(cleanup_queue_.empty());
}

void ExecNode::RowBatchQueue::AddBatch(RowBatch* batch) {
  //cout << PrintBatch(batch);
  if (!BlockingPut(batch)) {
    ScopedSpinLock l(&lock_);
    cleanup_queue_.push_back(batch);
  }
}

RowBatch* ExecNode::RowBatchQueue::GetBatch() {
  RowBatch* result = NULL;
  if (BlockingGet(&result)) return result;
  return NULL;
}

int ExecNode::RowBatchQueue::Cleanup() {
  int num_io_buffers = 0;

  RowBatch* batch = NULL;
  while ((batch = GetBatch()) != NULL) {
    num_io_buffers += batch->num_io_buffers();
    delete batch;
  }

  ScopedSpinLock l(&lock_);
  for (list<RowBatch*>::iterator it = cleanup_queue_.begin();
      it != cleanup_queue_.end(); ++it) {
    num_io_buffers += (*it)->num_io_buffers();
    delete *it;
  }
  cleanup_queue_.clear();
  return num_io_buffers;
}

ExecNode::ExecNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs)
  : id_(tnode.node_id),
    type_(tnode.node_type),
    pool_(pool),
    codegend_conjuncts_thread_safe_(true),
    row_descriptor_(descs, tnode.row_tuples, tnode.nullable_tuples),
    debug_phase_(TExecNodePhase::INVALID),
    debug_action_(TDebugAction::WAIT),
    limit_(tnode.limit),
    num_rows_returned_(0),
    rows_returned_counter_(NULL),
    rows_returned_rate_(NULL),
    is_closed_(false) {
  InitRuntimeProfile(PrintPlanNodeType(tnode.node_type));
}

Status ExecNode::Init(const TPlanNode& tnode) {
  return Expr::CreateExprTrees(pool_, tnode.conjuncts, &conjuncts_);
}

Status ExecNode::Prepare(RuntimeState* state) {
  RETURN_IF_ERROR(ExecDebugAction(TExecNodePhase::PREPARE, state));
  DCHECK(runtime_profile_.get() != NULL);
  rows_returned_counter_ =
      ADD_COUNTER(runtime_profile_, "RowsReturned", TCounterType::UNIT);
  mem_tracker_.reset(new MemTracker(
      runtime_profile_.get(), -1, runtime_profile_->name(),
      state->instance_mem_tracker()));

  rows_returned_rate_ = runtime_profile()->AddDerivedCounter(
      ROW_THROUGHPUT_COUNTER, TCounterType::UNIT_PER_SECOND,
      bind<int64_t>(&RuntimeProfile::UnitsPerSecond, rows_returned_counter_,
        runtime_profile()->total_time_counter()));

  RETURN_IF_ERROR(PrepareConjuncts(state));
  for (int i = 0; i < children_.size(); ++i) {
    RETURN_IF_ERROR(children_[i]->Prepare(state));
  }
  return Status::OK;
}

ExecNode::~ExecNode() {
}

void ExecNode::Close(RuntimeState* state) {
  if (is_closed_) return;
  is_closed_ = true;

  if (rows_returned_counter_ != NULL) {
    COUNTER_SET(rows_returned_counter_, num_rows_returned_);
  }
  for (int i = 0; i < children_.size(); ++i) {
    children_[i]->Close(state);
  }
  if (mem_tracker() != NULL) {
    DCHECK_EQ(mem_tracker()->consumption(), 0) << "Leaked memory.";
  }
}

void ExecNode::AddRuntimeExecOption(const string& str) {
  lock_guard<mutex> l(exec_options_lock_);
  if (runtime_exec_options_.empty()) {
    runtime_exec_options_ = str;
  } else {
    runtime_exec_options_.append(", ");
    runtime_exec_options_.append(str);
  }
  runtime_profile()->AddInfoString("ExecOption", runtime_exec_options_);
}

Status ExecNode::CreateTree(ObjectPool* pool, const TPlan& plan,
                            const DescriptorTbl& descs, ExecNode** root) {
  if (plan.nodes.size() == 0) {
    *root = NULL;
    return Status::OK;
  }
  int node_idx = 0;
  Status status = CreateTreeHelper(pool, plan.nodes, descs, NULL, &node_idx, root);
  if (status.ok() && node_idx + 1 != plan.nodes.size()) {
    status = Status(
        "Plan tree only partially reconstructed. Not all thrift nodes were used.");
  }
  if (!status.ok()) {
    LOG(ERROR) << "Could not construct plan tree:\n"
               << apache::thrift::ThriftDebugString(plan);
  }
  return status;
}

Status ExecNode::CreateTreeHelper(
    ObjectPool* pool,
    const vector<TPlanNode>& tnodes,
    const DescriptorTbl& descs,
    ExecNode* parent,
    int* node_idx,
    ExecNode** root) {
  // propagate error case
  if (*node_idx >= tnodes.size()) {
    return Status("Failed to reconstruct plan tree from thrift.");
  }
  int num_children = tnodes[*node_idx].num_children;
  ExecNode* node = NULL;
  RETURN_IF_ERROR(CreateNode(pool, tnodes[*node_idx], descs, &node));
  // assert(parent != NULL || (node_idx == 0 && root_expr != NULL));
  if (parent != NULL) {
    parent->children_.push_back(node);
  } else {
    *root = node;
  }
  for (int i = 0; i < num_children; i++) {
    ++*node_idx;
    RETURN_IF_ERROR(CreateTreeHelper(pool, tnodes, descs, node, node_idx, NULL));
    // we are expecting a child, but have used all nodes
    // this means we have been given a bad tree and must fail
    if (*node_idx >= tnodes.size()) {
      return Status("Failed to reconstruct plan tree from thrift.");
    }
  }

  // build up tree of profiles; add children >0 first, so that when we print
  // the profile, child 0 is printed last (makes the output more readable)
  for (int i = 1; i < node->children_.size(); ++i) {
    node->runtime_profile()->AddChild(node->children_[i]->runtime_profile());
  }
  if (!node->children_.empty()) {
    node->runtime_profile()->AddChild(node->children_[0]->runtime_profile(), false);
  }

  return Status::OK;
}

Status ExecNode::CreateNode(ObjectPool* pool, const TPlanNode& tnode,
                            const DescriptorTbl& descs, ExecNode** node) {
  stringstream error_msg;
  switch (tnode.node_type) {
    case TPlanNodeType::MAGIC_NODE:
      cout << "Using Pythia" << endl;
      *node = pool->Add(new PythiaReaderNode(pool, tnode, descs));
      break;
    case TPlanNodeType::HDFS_SCAN_NODE:
      //if (descs.GetTupleDescriptor(0)->table_desc()->name().find("pythia") != string::npos) {
        //cout << "Using Shared Memory Reader" << endl;
        //*node = pool->Add(new ShmScanNode(pool, tnode, descs));
      *node = pool->Add(new HdfsScanNode(pool, tnode, descs));
      break;
    case TPlanNodeType::HBASE_SCAN_NODE:
      *node = pool->Add(new HBaseScanNode(pool, tnode, descs));
      break;
    case TPlanNodeType::AGGREGATION_NODE:
      *node = pool->Add(new AggregationNode(pool, tnode, descs));
      break;
    case TPlanNodeType::HASH_JOIN_NODE:
      *node = pool->Add(new HashJoinNode(pool, tnode, descs));
      break;
    case TPlanNodeType::CROSS_JOIN_NODE:
      *node = pool->Add(new CrossJoinNode(pool, tnode, descs));
      break;
    case TPlanNodeType::EXCHANGE_NODE:
      *node = pool->Add(new ExchangeNode(pool, tnode, descs));
      break;
    case TPlanNodeType::SELECT_NODE:
      *node = pool->Add(new SelectNode(pool, tnode, descs));
      break;
    case TPlanNodeType::SORT_NODE:
      if (tnode.sort_node.use_top_n) {
        *node = pool->Add(new TopNNode(pool, tnode, descs));
      } else {
        // TODO: Need Sort Node
        //  *node = pool->Add(new SortNode(pool, tnode, descs));
        error_msg << "ORDER BY with no LIMIT not implemented";
        return Status(error_msg.str());
      }
      break;
    case TPlanNodeType::MERGE_NODE:
      *node = pool->Add(new MergeNode(pool, tnode, descs));
      break;
    default:
      map<int, const char*>::const_iterator i =
          _TPlanNodeType_VALUES_TO_NAMES.find(tnode.node_type);
      const char* str = "unknown node type";
      if (i != _TPlanNodeType_VALUES_TO_NAMES.end()) {
        str = i->second;
      }
      error_msg << str << " not implemented";
      return Status(error_msg.str());
  }
  RETURN_IF_ERROR((*node)->Init(tnode));
  return Status::OK;
}

void ExecNode::SetDebugOptions(
    int node_id, TExecNodePhase::type phase, TDebugAction::type action,
    ExecNode* root) {
  if (root->id_ == node_id) {
    root->debug_phase_ = phase;
    root->debug_action_ = action;
    return;
  }
  for (int i = 0; i < root->children_.size(); ++i) {
    SetDebugOptions(node_id, phase, action, root->children_[i]);
  }
}

string ExecNode::DebugString() const {
  stringstream out;
  this->DebugString(0, &out);
  return out.str();
}

void ExecNode::DebugString(int indentation_level, stringstream* out) const {
  *out << " conjuncts=" << Expr::DebugString(conjuncts_);
  for (int i = 0; i < children_.size(); ++i) {
    *out << "\n";
    children_[i]->DebugString(indentation_level + 1, out);
  }
}

Status ExecNode::PrepareConjuncts(RuntimeState* state) {
  codegend_conjuncts_thread_safe_ = true;
  for (vector<Expr*>::iterator i = conjuncts_.begin(); i != conjuncts_.end(); ++i) {
    bool is_thread_safe;
    RETURN_IF_ERROR(Expr::Prepare(*i, state, row_desc(), false, &is_thread_safe));
    codegend_conjuncts_thread_safe_ &= is_thread_safe;
  }
  return Status::OK;
}

bool ExecNode::EvalConjuncts(Expr* const* exprs, int num_exprs, TupleRow* row) {
  for (int i = 0; i < num_exprs; ++i) {
    void* value = exprs[i]->GetValue(row);
    if (value == NULL || *reinterpret_cast<bool*>(value) == false) return false;
  }
  return true;
}

// Codegen for EvalConjuncts.  The generated signature is
// For a node with two conjunct predicates
// define i1 @EvalConjuncts(%"class.impala::Expr"** %exprs, i32 %num_exprs,
//                          %"class.impala::TupleRow"* %row) {
// entry:
//   %null_ptr = alloca i1
//   %0 = bitcast %"class.impala::TupleRow"* %row to i8**
//   %eval = call i1 @BinaryPredicate(i8** %0, i8* null, i1* %null_ptr)
//   br i1 %eval, label %continue, label %false
//
// continue:                                         ; preds = %entry
//   %eval2 = call i1 @BinaryPredicate3(i8** %0, i8* null, i1* %null_ptr)
//   br i1 %eval2, label %continue1, label %false
//
// continue1:                                        ; preds = %continue
//   ret i1 true
//
// false:                                            ; preds = %continue, %entry
//   ret i1 false
// }
Function* ExecNode::CodegenEvalConjuncts(LlvmCodeGen* codegen,
    const vector<Expr*>& conjuncts) {
  for (int i = 0; i < conjuncts.size(); ++i) {
    if (conjuncts[i]->codegen_fn() == NULL) {
      VLOG_QUERY << "Could not codegen EvalConjuncts because one of the conjuncts "
                 << "could not be codegen'd.";
      return NULL;
    }
  }

  // Construct function signature to match
  // bool EvalConjuncts(Expr** exprs, int num_exprs, TupleRow* row)
  Type* tuple_row_type = codegen->GetType(TupleRow::LLVM_CLASS_NAME);
  Type* expr_type = codegen->GetType(Expr::LLVM_CLASS_NAME);

  DCHECK(tuple_row_type != NULL);
  DCHECK(expr_type != NULL);

  PointerType* tuple_row_ptr_type = PointerType::get(tuple_row_type, 0);
  PointerType* expr_ptr_type = PointerType::get(expr_type, 0);

  LlvmCodeGen::FnPrototype prototype(
      codegen, "EvalConjuncts", codegen->GetType(TYPE_BOOLEAN));
  prototype.AddArgument(
      LlvmCodeGen::NamedVariable("exprs", PointerType::get(expr_ptr_type, 0)));
  prototype.AddArgument(
      LlvmCodeGen::NamedVariable("num_exprs", codegen->GetType(TYPE_INT)));
  prototype.AddArgument(LlvmCodeGen::NamedVariable("row", tuple_row_ptr_type));

  LlvmCodeGen::LlvmBuilder builder(codegen->context());
  Value* args[3];
  Function* fn = prototype.GeneratePrototype(&builder, args);
  // Other args are unused.
  Value* tuple_row_arg = args[2];

  if (conjuncts.size() > 0) {
    LLVMContext& context = codegen->context();
    // The exprs type TupleRows as char** (instead of TupleRow* or Tuple**).  We
    // could plumb the expr codegen to know the tuples it will operate on.
    // TODO: think about doing that
    Type* tuple_row_llvm_type = PointerType::get(codegen->ptr_type(), 0);
    tuple_row_arg = builder.CreateBitCast(tuple_row_arg, tuple_row_llvm_type);
    BasicBlock* false_block = BasicBlock::Create(context, "false", fn);

    LlvmCodeGen::NamedVariable null_var("null_ptr", codegen->boolean_type());
    Value* is_null_ptr = codegen->CreateEntryBlockAlloca(fn, null_var);

    for (int i = 0; i < conjuncts.size(); ++i) {
      BasicBlock* true_block = BasicBlock::Create(context, "continue", fn, false_block);
      Function* conjunct_fn = conjuncts[i]->codegen_fn();
      DCHECK_EQ(conjuncts[i]->scratch_buffer_size(), 0);
      Value* expr_args[] = { tuple_row_arg, codegen->null_ptr_value(), is_null_ptr };

      // Ignore null result.  If null, expr's will return false which
      // is exactly the semantics for conjuncts
      Value* eval = builder.CreateCall(conjunct_fn, expr_args, "eval");
      builder.CreateCondBr(eval, true_block, false_block);

      // Set insertion point for continue/end
      builder.SetInsertPoint(true_block);
    }
    builder.CreateRet(codegen->true_value());

    builder.SetInsertPoint(false_block);
    builder.CreateRet(codegen->false_value());
  } else {
    builder.CreateRet(codegen->true_value());
  }

  return codegen->FinalizeFunction(fn);
}

void ExecNode::CollectNodes(TPlanNodeType::type node_type, vector<ExecNode*>* nodes) {
  if (type_ == node_type) nodes->push_back(this);
  for (int i = 0; i < children_.size(); ++i) {
    children_[i]->CollectNodes(node_type, nodes);
  }
}

void ExecNode::CollectScanNodes(vector<ExecNode*>* nodes) {
  CollectNodes(TPlanNodeType::HDFS_SCAN_NODE, nodes);
  CollectNodes(TPlanNodeType::HBASE_SCAN_NODE, nodes);
}

void ExecNode::InitRuntimeProfile(const string& name) {
  stringstream ss;
  ss << name << " (id=" << id_ << ")";
  runtime_profile_.reset(new RuntimeProfile(pool_, ss.str()));
  runtime_profile_->set_metadata(id_);
}

Status ExecNode::ExecDebugAction(TExecNodePhase::type phase, RuntimeState* state) {
  DCHECK(phase != TExecNodePhase::INVALID);
  if (debug_phase_ != phase) return Status::OK;
  if (debug_action_ == TDebugAction::FAIL) return Status(TStatusCode::INTERNAL_ERROR);
  if (debug_action_ == TDebugAction::WAIT) {
    while (!state->is_cancelled()) {
      sleep(1);
    }
    return Status::CANCELLED;
  }
  return Status::OK;
}

}
