// Author: Vikram

#include "exec/pythia-reader-node.h"

#include <boost/scoped_ptr.hpp>
#include <boost/thread/mutex.hpp>
#include <sstream>

#include "runtime/runtime-state.h"
#include "runtime/row-batch.h"
#include "util/debug-util.h"
#include "util/disk-info.h"
#include "util/runtime-profile.h"
#include "gen-cpp/PlanNodes_types.h"

using namespace impala;
using namespace std;
using namespace boost;
using namespace libconfig;

PythiaReaderNode::PythiaReaderNode(ObjectPool* pool, const TPlanNode& tnode, 
	                               const DescriptorTbl& descs)
    : ExecNode(pool, tnode, descs),
      runtime_state_(NULL),
      tuple_desc_(NULL),
      tuple_id_(tnode.hdfs_scan_node.tuple_id),
      tuple_(NULL),
      batch_(NULL),
      done_(false) {
  max_materialized_row_batches_ = 10 * DiskInfo::num_disks();
  materialized_row_batches_.reset(new RowBatchQueue(max_materialized_row_batches_));
}

PythiaReaderNode::~PythiaReaderNode() {
}

Status PythiaReaderNode::Prepare(RuntimeState* state) {
  runtime_state_ = state;
  RETURN_IF_ERROR(ExecNode::Prepare(state));

  tuple_desc_ = state->desc_tbl().GetTupleDescriptor(tuple_id_);
  DCHECK(tuple_desc_ != NULL);

  num_null_bytes_ = tuple_desc_->num_null_bytes();

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

void PythiaReaderNode::StartNewRowBatch() {
  batch_ = new RowBatch(row_desc(), runtime_state_->batch_size(), mem_tracker());
  tuple_mem_ =
      batch_->tuple_data_pool()->Allocate(runtime_state_->batch_size() * tuple_desc_->byte_size());
}

void PythiaReaderNode::fail(const char* explanation) {
  std::cout << " ** FAILED: " << explanation << std::endl;
  throw QueryExecutionError();
}

Status PythiaReaderNode::Compute() 
{
  Operator::GetNextResultT result; 
  result.first = Operator::Ready;
  stringstream stream;

  clock_t t1,t2;
  t1 = clock();

  if (q.scanStart() == Operator::Error)
    fail("Scan initialization failed.");

  while(result.first == Operator::Ready) {
    result = q.getNext();
    if (result.first == Operator::Error)
      fail("GetNext returned error.");

    StartNewRowBatch();

    Operator::Page::Iterator it = result.second->createIterator();
    void* ptuple;
    int num_rows;
    while (true) {
      MemPool* pool;
      TupleRow* tuple_row;
      int max_tuples = GetMemory(&pool, &tuple_, &tuple_row);

      DCHECK_GT(max_tuples, 0);
      DCHECK(tuple_ != NULL);
      num_rows = 0;

      uint8_t* tuple_row_mem = reinterpret_cast<uint8_t*>(tuple_row);
      uint8_t* tuple_mem = reinterpret_cast<uint8_t*>(tuple_);
      Tuple* tuple = reinterpret_cast<Tuple*>(tuple_mem);
      while ((ptuple = it.next())) {
        //stream.str(std::string());
        //stream << q.getOutSchema().prettyprint(ptuple, '|') << endl;
        //sscanf(stream.str().c_str(), "%ld\n", &val);
        int64_t val = *reinterpret_cast<int64_t*>(ptuple);
        
        // Materialize a single tuple
        if (WriteCompleteTuple(pool, tuple, tuple_row, val)) {
          num_rows++;
          tuple_mem += tuple_desc_->byte_size();
          tuple_row_mem += batch_->row_byte_size();
          tuple = reinterpret_cast<Tuple*>(tuple_mem);
          tuple_row = reinterpret_cast<TupleRow*>(tuple_row_mem);
        }

        // If the current batch is full, commit the rows
        if (num_rows == max_tuples) {
          RETURN_IF_ERROR(CommitRows(num_rows));
          break;
        }
      }
      DCHECK_LE(num_rows, max_tuples);
      if (num_rows != max_tuples) {
        RETURN_IF_ERROR(CommitLastRows(num_rows));
        break;
      }
    }
  }

  if (q.scanStop() == Operator::Error) 
    fail("Scan stop failed.");

  SetDone();

  t2 = clock();
  float diff ((float)t2-(float)t1);
  cout << "ResponseTimeInSec: " << diff / CLOCKS_PER_SEC << endl;

  return Status::OK;
}

void PythiaReaderNode::ScannerThread() {
  Config cfg;

  clock_t t1,t2;
  t1 = clock();

  cfg.readFile("/home/vikram/pythia/drivers/test.conf");
  q.create(cfg);

  t2 = clock();
  float diff ((float)t2-(float)t1);
  cout << "CreateConfigTimeInSec: " << diff / CLOCKS_PER_SEC << endl;

  q.threadInit();

  t1 = clock();
  diff = ((float)t1-(float)t2);
  cout << "ThreadInitTimeInSec: " << diff / CLOCKS_PER_SEC << endl;

  Compute();

  q.threadClose();

  q.destroy();
}

Status PythiaReaderNode::Open(RuntimeState* state) {
  thread =
    new Thread("pythia-reader-node", "scanner-thread(1)", &PythiaReaderNode::ScannerThread, this);
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

void PythiaReaderNode::Close(RuntimeState* state) {
  if (is_closed()) return;
  SetDone();

  thread->Join();

  num_owned_io_buffers_ -= materialized_row_batches_->Cleanup();
  DCHECK_EQ(num_owned_io_buffers_, 0) << "ScanNode has leaked io buffers";

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

bool PythiaReaderNode::WriteCompleteTuple(MemPool* pool, Tuple* tuple, 
                  TupleRow* tuple_row, int64_t val) {
  // Initialize tuple before materializing slots
  InitTuple(tuple);

  for (int i = 0; i < materialized_slots_.size(); ++i) {
    SlotDescriptor* desc = materialized_slots_[i];
    
    void* slot = tuple->GetSlot(desc->tuple_offset());
    *reinterpret_cast<int64_t*>(slot) = val;
  }

  tuple_row->SetTuple(tuple_idx(), tuple);
  return true;
}

Status PythiaReaderNode::CommitRows(int num_rows) {
  DCHECK(batch_ != NULL);
  DCHECK_LE(num_rows, batch_->capacity() - batch_->num_rows());
  batch_->CommitRows(num_rows);
  tuple_mem_ += tuple_desc_->byte_size() * num_rows;

  if (batch_->IsFull() || batch_->AtResourceLimit()) {
    materialized_row_batches_->AddBatch(batch_);
    StartNewRowBatch();
  }

  return Status::OK;
}

Status PythiaReaderNode::CommitLastRows(int num_rows) {
  DCHECK(batch_ != NULL);
  DCHECK_LE(num_rows, batch_->capacity() - batch_->num_rows());
  batch_->CommitRows(num_rows);
  tuple_mem_ += tuple_desc_->byte_size() * num_rows;

  materialized_row_batches_->AddBatch(batch_);
  return Status::OK;
}
