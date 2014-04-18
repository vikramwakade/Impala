// Author: Vikram

#include "exec/shm-scan-node.h"

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

ShmScanNode::ShmScanNode(ObjectPool* pool, const TPlanNode& tnode, 
	                               const DescriptorTbl& descs)
    : ExecNode(pool, tnode, descs),
      runtime_state_(NULL),
      tuple_desc_(NULL),
      tuple_id_(tnode.hdfs_scan_node.tuple_id),
      tuple_(NULL),
      batch_(NULL),
      done_(false) {
  max_materialized_row_batches_ = 1;
  materialized_row_batches_.reset(new RowBatchQueue(max_materialized_row_batches_));
}

ShmScanNode::~ShmScanNode() {
}

Status ShmScanNode::Prepare(RuntimeState* state) {
  runtime_state_ = state;
  RETURN_IF_ERROR(ExecNode::Prepare(state));

  tuple_desc_ = state->desc_tbl().GetTupleDescriptor(tuple_id_);
  DCHECK(tuple_desc_ != NULL);

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

void ShmScanNode::StartNewRowBatch() {
  batch_ = new RowBatch(row_desc(), runtime_state_->batch_size(), mem_tracker());
  tuple_mem_ =
      batch_->tuple_data_pool()->Allocate(runtime_state_->batch_size() * tuple_desc_->byte_size());
}

Status ShmScanNode::Open(RuntimeState* state) {
  thread = new Thread("shm-scan-node", "scanner-thread(1)", &ShmScanNode::ScannerThread, this);
  return Status::OK;
}

void ShmScanNode::ScannerThread() {
  AtomicUtil::MemoryBarrier();

  StartNewRowBatch();
  int i = 0;
  while (i < 20) {
    MemPool* pool;
    TupleRow* tuple_row;
    int max_tuples = GetMemory(&pool, &tuple_, &tuple_row);

    DCHECK_GT(max_tuples, 0);

    int num_tuples_materialized = 0;
    if (materialized_slots_.size() != 0) {
      num_tuples_materialized = WriteAlignedTuples(pool, tuple_row, max_tuples);
    } else {
      num_tuples_materialized = WriteEmptyTuples(tuple_row, max_tuples);
    }
    // Commit the rows to the row batch and scan node
    if (i == 19)
      CommitRows(num_tuples_materialized, true);
    else
      CommitRows(num_tuples_materialized, false);
    i++;
  }
  SetDone();
}

int ShmScanNode::GetMemory(MemPool** pool, Tuple** tuple_mem, TupleRow** tuple_row_mem) {
  DCHECK(batch_ != NULL);
  DCHECK(!batch_->IsFull());
  *pool = batch_->tuple_data_pool();
  *tuple_mem = reinterpret_cast<Tuple*>(tuple_mem_);
  *tuple_row_mem = batch_->GetRow(batch_->AddRow());
  return batch_->capacity() - batch_->num_rows();
}

int ShmScanNode::WriteAlignedTuples(MemPool* pool, TupleRow* tuple_row, int num_tuples) {

  DCHECK(tuple_ != NULL);

  uint8_t* tuple_row_mem = reinterpret_cast<uint8_t*>(tuple_row);
  uint8_t* tuple_mem = reinterpret_cast<uint8_t*>(tuple_);
  Tuple* tuple = reinterpret_cast<Tuple*>(tuple_mem);

  for (int counter = 0; counter < num_tuples; counter++) {
    // Materialize a single tuple.
    if (WriteCompleteTuple(pool, tuple, tuple_row, counter)) {
      tuple_mem += tuple_desc_->byte_size();
      tuple_row_mem += batch_->row_byte_size();
      tuple = reinterpret_cast<Tuple*>(tuple_mem);
      tuple_row = reinterpret_cast<TupleRow*>(tuple_row_mem);
    }
  }

  return num_tuples;
}

int ShmScanNode::WriteEmptyTuples(TupleRow* tuple_row, int num_tuples) {
  return num_tuples;
  // Commented since template tuples are not implemented
  /*row->SetTuple(tuple_idx(), template_tuple_);
  if (!ExecNode::EvalConjuncts(&(*conjuncts_)[0], num_conjuncts_, row)) return 0;
  row = next_row(row);

  for (int n = 1; n < num_tuples; ++n) {
    row->SetTuple(tuple_idx(), template_tuple_);
    row = next_row(row);
  }*/
}

bool ShmScanNode::WriteCompleteTuple(MemPool* pool, Tuple* tuple, TupleRow* tuple_row, int val) {

  for (int i = 0; i < materialized_slots_.size(); ++i) {
    SlotDescriptor* desc = materialized_slots_[i];
    
    void* slot = tuple->GetSlot(desc->tuple_offset());
    *reinterpret_cast<int32_t*>(slot) = val;
  }

  tuple_row->SetTuple(tuple_idx(), tuple);
  return true;
}

Status ShmScanNode::CommitRows(int num_rows, bool end) {
  DCHECK(batch_ != NULL);
  DCHECK_LE(num_rows, batch_->capacity() - batch_->num_rows());
  batch_->CommitRows(num_rows);
  tuple_mem_ += tuple_desc_->byte_size() * num_rows;

  if (batch_->IsFull() || batch_->AtResourceLimit()) {
    materialized_row_batches_->AddBatch(batch_);
    if (!end)
      StartNewRowBatch();
  }

  return Status::OK;
}

Status ShmScanNode::GetNext(RuntimeState* state, RowBatch* row_batch, bool* eos) {
  Status status = GetNextInternal(state, row_batch, eos);
  if (status.IsMemLimitExceeded()) state->SetMemLimitExceeded();
  return status;
}

Status ShmScanNode::GetNextInternal(RuntimeState* state, RowBatch* row_batch, bool* eos) {
  RETURN_IF_ERROR(ExecDebugAction(TExecNodePhase::GETNEXT, state));
  RETURN_IF_CANCELLED(state);
  RETURN_IF_ERROR(state->CheckQueryState());
  SCOPED_TIMER(runtime_profile_->total_time_counter());

  RowBatch* materialized_batch = materialized_row_batches_->GetBatch();
  if (materialized_batch != NULL) {
    num_owned_io_buffers_ -= materialized_batch->num_io_buffers();
    row_batch->AcquireState(materialized_batch);
    // Update the number of materialized rows instead of when they are materialized.
    // This means that scanners might process and queue up more rows than are necessary
    // for the limit case but we want to avoid the synchronized writes to
    // num_rows_returned_
    num_rows_returned_ += row_batch->num_rows();
    COUNTER_SET(rows_returned_counter_, num_rows_returned_);
    
    if (ReachedLimit()) {
      int num_rows_over = num_rows_returned_ - limit_;
      row_batch->set_num_rows(row_batch->num_rows() - num_rows_over);
      num_rows_returned_ -= num_rows_over;
      COUNTER_SET(rows_returned_counter_, num_rows_returned_);

      *eos = true;
      SetDone();
    }
    DCHECK_EQ(materialized_batch->num_io_buffers(), 0);
    delete materialized_batch;
    *eos = false;
    return Status::OK;
  }

  *eos = true;
  return Status::OK;
}

void ShmScanNode::Close(RuntimeState* state) {
  if (is_closed()) return;
  SetDone();

  thread->Join();

  num_owned_io_buffers_ -= materialized_row_batches_->Cleanup();
  DCHECK_EQ(num_owned_io_buffers_, 0) << "ScanNode has leaked io buffers";

  ExecNode::Close(state);
}

void ShmScanNode::SetDone() {
  {
    unique_lock<mutex> l(lock_);
    if (done_) return;
    done_ = true;
  }
  materialized_row_batches_->Shutdown();
}
