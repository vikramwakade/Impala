// Author: Vikram

#include "exec/pythia-reader-node.h"

#include <boost/scoped_ptr.hpp>
#include <boost/thread/mutex.hpp>

#include "runtime/runtime-state.h"
#include "runtime/row-batch.h"
#include "util/debug-util.h"
#include "util/runtime-profile.h"
#include "gen-cpp/PlanNodes_types.h"

using namespace impala;
using namespace std;
using namespace boost;

PythiaReaderNode::PythiaReaderNode(ObjectPool* pool, const TPlanNode& tnode, 
	                               const DescriptorTbl& descs)
    : ExecNode(pool, tnode, descs),
      tuple_id_(tnode.hdfs_scan_node.tuple_id),
      batch_(NULL),
      tuple_(NULL),
      done_(false),
      tuple_desc_(NULL) {
  max_materialized_row_batches_ = 10;
  materialized_row_batches_.reset(new RowBatchQueue(max_materialized_row_batches_));
}

PythiaReaderNode::~PythiaReaderNode() {
}

Status PythiaReaderNode::Prepare(RuntimeState* state) {
  RETURN_IF_ERROR(ExecNode::Prepare(state));
  tuple_desc_ = state->desc_tbl().GetTupleDescriptor(tuple_id_);
  batch_ = new RowBatch(row_desc(), state->batch_size(), mem_tracker());
  tuple_mem_ =
      batch_->tuple_data_pool()->Allocate(state->batch_size() * tuple_desc_->byte_size());

  // Create mapping from column index in table to slot index in output tuple.
  // First, initialize all columns to SKIP_COLUMN.
  int num_cols = tuple_desc_->table_desc()->num_cols();
  column_idx_to_materialized_slot_idx_.resize(num_cols);
  for (int i = 0; i < num_cols; ++i) {
    column_idx_to_materialized_slot_idx_[i] = SKIP_COLUMN;
  }

  // Next, collect all materialized slots
  vector<SlotDescriptor*> all_materialized_slots;
  all_materialized_slots.resize(num_cols);
  const vector<SlotDescriptor*>& slots = tuple_desc_->slots();
  for (size_t i = 0; i < slots.size(); ++i) {
    if (!slots[i]->is_materialized()) continue;
    int col_idx = slots[i]->col_pos();
    DCHECK_LT(col_idx, column_idx_to_materialized_slot_idx_.size());
    DCHECK_EQ(column_idx_to_materialized_slot_idx_[col_idx], SKIP_COLUMN);
    all_materialized_slots[col_idx] = slots[i];
  }

  // Finally, populate materialized_slots_ in the order that
  // the slots appear in the file.
  for (int i = 0; i < num_cols; ++i) {
    SlotDescriptor* slot_desc = all_materialized_slots[i];
    if (slot_desc == NULL) continue;
    else {
      column_idx_to_materialized_slot_idx_[i] = materialized_slots_.size();
      materialized_slots_.push_back(slot_desc);
    }
  }

  return Status::OK;
}

Status PythiaReaderNode::Open(RuntimeState* state) {
  MemPool* pool;
  TupleRow* tuple_row;
  int max_tuples = GetMemory(&pool, &tuple_, &tuple_row);

  DCHECK_GT(max_tuples, 0);
  DCHECK(tuple_ != NULL);

  uint8_t* tuple_row_mem = reinterpret_cast<uint8_t*>(tuple_row);
  uint8_t* tuple_mem = reinterpret_cast<uint8_t*>(tuple_);
  Tuple* tuple = reinterpret_cast<Tuple*>(tuple_mem);

  for (int counter = 1; counter <= 100; counter++) {
    // Materialize a single tuple.
    if (WriteCompleteTuple(pool, tuple, tuple_row, counter)) {
      tuple_mem += tuple_desc_->byte_size();
      tuple_row_mem += batch_->row_byte_size();
      tuple = reinterpret_cast<Tuple*>(tuple_mem);
      tuple_row = reinterpret_cast<TupleRow*>(tuple_row_mem);
    }
  }
  // Commit the rows to the row batch and scan node
  RETURN_IF_ERROR(CommitRows(100));

  return Status::OK;
}

int PythiaReaderNode::GetMemory(MemPool** pool, Tuple** tuple_mem, TupleRow** tuple_row_mem) {
  DCHECK(batch_ != NULL);
  DCHECK(!batch_->IsFull());
  *pool = batch_->tuple_data_pool();
  *tuple_mem = reinterpret_cast<Tuple*>(tuple_mem_);
  *tuple_row_mem = batch_->GetRow(batch_->AddRow());
  return batch_->capacity() - batch_->num_rows();
}

Status PythiaReaderNode::GetNext(RuntimeState* state, RowBatch* row_batch, bool* eos) {
  Status status = GetNextInternal(state, row_batch, eos);
  if (status.IsMemLimitExceeded()) state->SetMemLimitExceeded();
  return status;
}

Status PythiaReaderNode::GetNextInternal(RuntimeState* state, RowBatch* row_batch, bool* eos) {
  RETURN_IF_ERROR(ExecDebugAction(TExecNodePhase::GETNEXT, state));
  RETURN_IF_CANCELLED(state);
  RETURN_IF_ERROR(state->CheckQueryState());
  SCOPED_TIMER(runtime_profile_->total_time_counter());

  RowBatch* materialized_batch = materialized_row_batches_->GetBatch();
  if (materialized_batch != NULL) {
    row_batch->AcquireState(materialized_batch);
    
    DCHECK_EQ(materialized_batch->num_io_buffers(), 0);
    delete materialized_batch;
    //*eos = false;
    //return Status::OK;
    SetDone();
  }

  *eos = true;
  return Status::OK;
}

void PythiaReaderNode::Close(RuntimeState* state) {
  if (is_closed()) return;
  SetDone();

  ExecNode::Close(state);
}

void PythiaReaderNode::SetDone() {
  {
    unique_lock<mutex> l(lock_);
    if (done_) return;
    done_ = true;
  }
  materialized_row_batches_->Shutdown();
}

bool PythiaReaderNode::WriteCompleteTuple(MemPool* pool, Tuple* tuple, TupleRow* tuple_row, int val) {

  for (int i = 0; i < materialized_slots_.size(); ++i) {
    SlotDescriptor* desc = materialized_slots_[i];
    
    void* slot = tuple->GetSlot(desc->tuple_offset());
    *reinterpret_cast<int32_t*>(slot) = val;
  }

  tuple_row->SetTuple(tuple_idx(), tuple);
  return true;
}

Status PythiaReaderNode::CommitRows(int num_rows) {
  DCHECK(batch_ != NULL);
  DCHECK_LE(num_rows, batch_->capacity() - batch_->num_rows());
  batch_->CommitRows(num_rows);
  tuple_mem_ += tuple_desc_->byte_size() * num_rows;

  // Ideally should check if the batch is full but here we are not 
  // adding any more rows, so can push the batch_ in the queue
  materialized_row_batches_->AddBatch(batch_);

  return Status::OK;
}
