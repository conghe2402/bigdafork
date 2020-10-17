package com.opens.bigdafork.miner.japi;

import com.opens.bigdafork.miner.MinerDesc;
import com.opens.bigdafork.miner.MinerTool;
import com.opens.bigdafork.miner.exception.MinerException;
import com.opens.bigdafork.miner.tools.lostvalue.CorrectType;
import com.opens.bigdafork.miner.tools.lostvalue.LostValFuncParams;
import scala.Enumeration.Value;
import scala.Tuple4;
import scala.collection.Seq;

import java.util.ArrayList;
import java.util.List;

/**
 * JavaMinerDesc.
 */
public final class JavaMinerDesc {
    /**
     * getLostValFuncDesc.
     * @return
     */
    public static LostValFuncDescHelper getLostValFuncDescHelper() {
        return LostValFuncDescHelper.getInstance();
    }

    /**
     * Encapsulation of Lost Value Func Desc.
     */
    public static final class LostValFuncDescHelper {
        private static final LostValFuncDescHelper INSTANCE = new LostValFuncDescHelper();

        public static final String MEAN = "0";

        public MinerTool getLostValFuncTool() {
            return MinerDesc.LostValFuncDesc$.MODULE$.getTool();
        }

        public MinerTool getLostValFuncTool(boolean debug) {
            return MinerDesc.LostValFuncDesc$.MODULE$.getTool(debug);
        }

        public LostValFuncParams needLostValFuncToolParams() {
            return MinerDesc.LostValFuncDesc$.MODULE$.needParam();
        }

        /**
         * setLostValFuncToolParams.
         * @param tool
         * @param params
         * @param lostValueFuncDefList List<String[]> :
         *         {[col_id, col_name, lost value definition, lost value handle method]}
         *          col_id is number , its value is based on zero.
         */
        public void setLostValFuncToolParams(MinerTool tool,
                                         LostValFuncParams params,
                                         List<String[]> lostValueFuncDefList) {
            if (lostValueFuncDefList == null || lostValueFuncDefList.size() <= 0) {
                return;
            }

            List<Tuple4<Integer, String, String, scala.Enumeration.Value>> paramsList
                    = new ArrayList<>(lostValueFuncDefList.size());
            for (int i = 0; i < lostValueFuncDefList.size(); i++) {
                paramsList.add(new Tuple4(Integer.parseInt(lostValueFuncDefList.get(i)[0]),
                        lostValueFuncDefList.get(i)[1],
                        lostValueFuncDefList.get(i)[2],
                        getLostFuncCorrectType(lostValueFuncDefList.get(i)[3])));
            }

            params.setJLostValueFuncDef(paramsList);
            tool.setParams(params);

        }

        private Value getLostFuncCorrectType(String type) {
            if (type.equals(MEAN)) {
                return CorrectType.MEAN();
            } else {
                List<String> mesList = new ArrayList();
                mesList.add(String.format("%s is not support.", type));

                throw new MinerException((Seq<String>) mesList);
            }
        }

        private static LostValFuncDescHelper getInstance() {
            return INSTANCE;
        }

        private LostValFuncDescHelper() {}
    }

    private JavaMinerDesc() {}
}
