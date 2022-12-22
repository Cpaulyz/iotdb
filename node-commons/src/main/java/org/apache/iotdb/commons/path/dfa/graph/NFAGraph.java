/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.commons.path.dfa.graph;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.dfa.DFAState;
import org.apache.iotdb.commons.path.fa.IFAState;
import org.apache.iotdb.commons.path.fa.IFATransition;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class NFAGraph {
  private final List<IFAState> nfaStateList = new ArrayList<>();

  // [transitionIndex][stateIndex]List<IFAState>
  private final List<IFAState>[][] nfaTransitionTable;
  //  private final Map<IFATransition, List<List<IFAState>>> nfaTransitionTable = new HashMap<>();

  public NFAGraph(
      PartialPath pathPattern, boolean isPrefix, Map<String, IFATransition> transitionMap) {
    nfaTransitionTable = new List[transitionMap.size()][pathPattern.getNodeLength() + 1];
    // init start state, curNodeIndex=0
    int curStateIndex = 0;
    nfaStateList.add(new DFAState(curStateIndex));
    //    for(IFATransition transition:transitionMap.values()){
    //      nfaTransitionTable.put(transition,new ArrayList<>());
    //      nfaTransitionTable.get(transition).add(new ArrayList<>());
    //    }
    for (int i = 0; i < transitionMap.size(); i++) {
      nfaTransitionTable[i][0] = new ArrayList<>();
    }
    // traverse pathPattern and construct NFA
    for (int i = 0; i < pathPattern.getNodeLength(); i++) {
      String node = pathPattern.getNodes()[i];
      // if it is tail node, transit to final state
      DFAState state =
          i == pathPattern.getNodeLength() - 1
              ? new DFAState(++curStateIndex, true)
              : new DFAState(++curStateIndex);
      nfaStateList.add(state);
      //      for(IFATransition transition:transitionMap.values()){
      //        nfaTransitionTable.get(transition).add(new ArrayList<>());
      //      }
      for (int j = 0; j < transitionMap.size(); j++) {
        nfaTransitionTable[j][curStateIndex] = new ArrayList<>();
      }
      // construct transition
      if (IoTDBConstant.ONE_LEVEL_PATH_WILDCARD.equals(node)) {
        for (IFATransition transition : transitionMap.values()) {
          //          nfaTransitionTable.get(transition).get(curStateIndex-1).add(state);
          nfaTransitionTable[transition.getIndex()][curStateIndex - 1].add(state);
        }
      } else if (IoTDBConstant.MULTI_LEVEL_PATH_WILDCARD.equals(node)) {
        for (IFATransition transition : transitionMap.values()) {
          //          nfaTransitionTable.get(transition).get(curStateIndex-1).add(state);
          //          nfaTransitionTable.get(transition).get(curStateIndex).add(state);
          nfaTransitionTable[transition.getIndex()][curStateIndex - 1].add(state);
          nfaTransitionTable[transition.getIndex()][curStateIndex].add(state);
        }
      } else {
        //        nfaTransitionTable.get(transitionMap.get(node)).get(curStateIndex-1).add(state);
        nfaTransitionTable[transitionMap.get(node).getIndex()][curStateIndex - 1].add(state);
      }
      if (isPrefix && i == pathPattern.getNodeLength() - 1) {
        for (IFATransition transition : transitionMap.values()) {
          //          nfaTransitionTable.get(transition).get(curStateIndex).add(state);
          nfaTransitionTable[transition.getIndex()][curStateIndex].add(state);
        }
      }
    }
  }

  public void print(Map<String, IFATransition> transitionMap) {
    System.out.println();
    System.out.println();
    System.out.println("NFA:");
    System.out.println(
        "==================================================================================================");
    StringBuilder stringBuilder = new StringBuilder();
    stringBuilder.append(String.format("|%-15s|", "State"));
    for (IFATransition transfer : transitionMap.values()) {
      stringBuilder.append(String.format("%-15s|", transfer.toString()));
    }
    stringBuilder.append(String.format("%-15s|", "Final"));
    System.out.println(stringBuilder);
    for (int i = 0; i < nfaStateList.size(); i++) {
      stringBuilder = new StringBuilder();
      stringBuilder.append(String.format("|%-15d|", i));
      for (IFATransition transition : transitionMap.values()) {
        //        stringBuilder.append(
        //            String.format(
        //                "%-15s|",
        //                StringUtils.join(
        //                    nfaTransitionTable[transition.getIndex()][i].stream()
        //                        .map(IFAState::getIndex)
        //                        .collect(Collectors.toList()),
        //                    ",")));
      }
      stringBuilder.append(String.format("%-15s|", nfaStateList.get(i).isFinal()));
      System.out.println(stringBuilder);
    }
  }

  public List<IFAState> getTransitions(IFATransition transition, IFAState state) {
    return nfaTransitionTable[transition.getIndex()][state.getIndex()];
    //    return nfaTransitionTable.get(transition).get(state.getIndex());
  }

  public int getStateSize() {
    return nfaStateList.size();
  }
}
