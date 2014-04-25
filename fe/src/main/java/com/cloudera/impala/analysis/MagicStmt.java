// Copyright (c) 2012 Cloudera, Inc. All rights reserved.
package com.cloudera.impala.analysis;

import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.cloudera.impala.common.AnalysisException;


/**
 * MagicStmt.
 *
 */
public class MagicStmt extends SelectStmt {
  private final static Logger LOG = LoggerFactory.getLogger(MagicStmt.class);
//  private final List<TableRef> tableRefs_;
  private long MAGIC_N=10;
//  private String sqlString_;
//  private final ArrayList<String> colLabels_;
  /**
   * Constructor (called from parser?)
   * @param tableRefList
   * @param n
   */
  MagicStmt(List<TableRef> tableRefList, Integer n) {
    super(null, null, null, null, null, null, null);
//    this.tableRefs_ = tableRefList;
    this.MAGIC_N = n;
//    this.colLabels_ = Lists.newArrayList();
  }
 MagicStmt(List<TableRef> tableRefList, SelectList selectList){
    super(selectList, tableRefList, null, null, null, null, null);
    // System.out.println("*********MAGIC Constructor *********"+selectList.getItems().get(0).getExpr().toString());
    IntLiteral il = (IntLiteral) selectList.getItems().get(0).getExpr();
    System.out.println("%%%%% MAGIC VALUE %%%%% = "+il.getValue());
    MAGIC_N = il.getValue();
  }

   public long getMagicNum(){
   return MAGIC_N;
 }

 

 MagicStmt(SelectStmt stmt){
   super(stmt.getSelectList(), stmt.getTableRefs(), stmt.getWhereClause(), null, null, null, null);
 }

//  @Override
//  public ArrayList<String> getColLabels() {
//    return colLabels_;
//  }
//
//  public List<TableRef> getTableRefs() { return tableRefs_; }
//
//  @Override
//  protected void substituteOrdinals(List<Expr> exprs, String errorPrefix) throws AnalysisException {
//  }
//
//  @Override
//  public void getMaterializedTupleIds(ArrayList<TupleId> tupleIdList) {
//    for (TableRef tblRef: tableRefs_) {
//      tupleIdList.addAll(tblRef.getMaterializedTupleIds());
//    }
//  }
//
//  @Override
//  public void materializeRequiredSlots(Analyzer analyzer) {
//    // TODO Auto-generated method stub
//
//  }
//
//  private ArrayList<TableRef> cloneTableRefs() {
//    ArrayList<TableRef> clone = Lists.newArrayList();
//    for (TableRef tblRef : tableRefs_) {
//      clone.add(tblRef.clone());
//    }
//    return clone;
//  }
//
//  @Override
//  public QueryStmt clone() {
//    return new MagicStmt(cloneTableRefs(), new Integer(this.MAGIC_N));
//  }

}

