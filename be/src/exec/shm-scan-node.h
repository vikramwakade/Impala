// Author: Vikram

#ifndef IMPALA_EXEC_SHM_SCAN_NODE_H_
#define IMPALA_EXEC_SHM_SCAN_NODE_H_

#include <vector>
#include <memory>
#include <stdint.h>

#include <boost/scoped_ptr.hpp>

#include "exec/exec-node.h"
#include "runtime/tuple.h"
#include "runtime/tuple-row.h"
#include "runtime/descriptors.h"
#include "runtime/string-buffer.h"
#include "util/thread.h"

#include "gen-cpp/PlanNodes_types.h"

namespace impala {

class MemPool;
class SlotDescriptor;
class Status;
class Tuple;
class TupleDescriptor;
class TPlanNode;

// Node to be used to read the output generated by Pythia and pass it up in the plan tree.
// Currently used to generate dummy tuples from 1 to k, k must be passed as a parameter
// from fe in tnode.
class ShmScanNode : public ExecNode {
 public:
  ShmScanNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs);

  ~ShmScanNode();

  // ExecNode methods
  virtual Status Prepare(RuntimeState* state);
  // Create and queue up the rowbatches in the RowBatchQueue
  virtual Status Open(RuntimeState* state);
  // GetNext will call GetNextInternal to dequeue the rowbatch from the RowBatchQueue
  virtual Status GetNext(RuntimeState* state, RowBatch* row_batch, bool* eos);
  virtual void Close(RuntimeState* state);

  // Returns the tuple idx into the row for this scan node to output to.
  // Currently this is always 0.
  int tuple_idx() const { return 0; }

  const static int SKIP_COLUMN = -1;

 private:
  RuntimeState* runtime_state_;

  // Descriptor for tuples this node constructs
  const TupleDescriptor* tuple_desc_;

  // Tuple id resolved in Prepare() to set tuple_desc_;
  const int tuple_id_;

  // Number of null bytes in the tuple.
  int32_t num_null_bytes_;

  // The tuple memory of batch_.
  uint8_t* tuple_mem_;

  // Current tuple pointer into tuple_mem_.
  Tuple* tuple_;

  // Vector containing slot descriptors for all materialized non-partition key
  // slots. These descriptors are sorted in order of increasing col_pos
  std::vector<SlotDescriptor*> materialized_slots_;

  // Maximum size of materialized_row_batches_.
  int max_materialized_row_batches_;

  // Outgoing row batches queue. Row batches are produced asynchronously by the scanner
  // threads and consumed by the main thread.
  boost::scoped_ptr<RowBatchQueue> materialized_row_batches_;

  // The current row batch being populated.
  RowBatch* batch_;

  // Vector containing indices into materialized_slots_.  The vector is indexed by
  // the slot_desc's col_pos.  Non-materialized slots will have SKIP_COLUMN as its entry.
  std::vector<int> column_idx_to_materialized_slot_idx_;

  // This is the number of io buffers that are owned by the scan node and the scanners.
  // This is used just to help debug leaked io buffers to determine if the leak is
  // happening in the scanners vs other parts of the execution.
  AtomicInt<int> num_owned_io_buffers_;

  // Lock protects access between scanner thread and main query thread (the one calling
  // GetNext()) for all fields below.  If this lock and any other locks needs to be taken
  // together, this lock must be taken first.
  boost::mutex lock_;

  // Flag signaling that all scanner threads are done.  This could be because they
  // are finished, an error/cancellation occurred, or the limit was reached.
  // Setting this to true triggers the scanner threads to clean up.
  // This should not be explicitly set. Instead, call SetDone().
  bool done_;

  // Thread to scan the shared memory file
  Thread* thread;

  // Checks for eos conditions and returns batches from materialized_row_batches_.
  Status GetNextInternal(RuntimeState* state, RowBatch* row_batch, bool* eos);

  // Set batch_ to a new row batch and update tuple_mem_ accordingly.
  void StartNewRowBatch();

  // Main function for scanner thread. This thread pulls the next range to be
  // processed from shared memory and then processes the entire range end to end.
  // This thread terminates when all the data from shared memory table has been read.
  void ScannerThread();

  // Gets memory for outputting tuples into batch_.
  //  *pool is the mem pool that should be used for memory allocated for those tuples.
  //  *tuple_mem should be the location to output tuples, and
  //  *tuple_row_mem for outputting tuple rows.
  // Returns the maximum number of tuples/tuple rows that can be output (before the
  // current row batch is complete and a new one is allocated).
  // Memory returned from this call is invalidated after calling CommitRows.  Callers must
  // call GetMemory again after calling this function.
  int GetMemory(MemPool** pool, Tuple** tuple_mem, TupleRow** tuple_row_mem);

  int WriteAlignedTuples(MemPool* pool, TupleRow* tuple_row, int num_tuples);

  int WriteEmptyTuples(TupleRow* tuple_row, int num_tuples);

  bool WriteCompleteTuple(MemPool* pool, Tuple* tuple, TupleRow* tuple_row, int32_t val);

  // Initialize a tuple.
  // Assumption - there are no null slots!
  void InitTuple(Tuple* tuple) {
    memset(tuple, 0, sizeof(uint8_t) * num_null_bytes_);
  }

  // Commit num_rows to the current row batch.  If this completes the row batch, the
  // row batch is enqueued with the scan node and StartNewRowBatch is called.
  // Returns Status::OK if the query is not cancelled and hasn't exceeded any mem limits.
  Status CommitRows(int num_rows, bool end);

  // sets done_ to true and triggers threads to cleanup. Cannot be calld with
  // any locks taken. Calling it repeatedly ignores subsequent calls.
  void SetDone();
};

}

#endif