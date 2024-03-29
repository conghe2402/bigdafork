package com.opens.bigdafork.miner.japi;

import com.opens.bigdafork.miner.MinerDesc;
import com.opens.bigdafork.miner.MinerTool;
import com.opens.bigdafork.miner.exception.MinerException;
import com.opens.bigdafork.miner.tools.binary.BinarizerFuncParams;
import com.opens.bigdafork.miner.tools.bucetizer.BucketizerFuncParams;
import com.opens.bigdafork.miner.tools.lostvalue.CorrectType;
import com.opens.bigdafork.miner.tools.lostvalue.LostValFuncParams;
import com.opens.bigdafork.miner.tools.normalize.NormFuncParams;
import com.opens.bigdafork.miner.tools.scaler.ScalerFuncParams;
import com.opens.bigdafork.miner.tools.scaler.ScalerType;

import org.apache.commons.beanutils.ConvertUtils;
import scala.Enumeration.Value;
import scala.Tuple4;
import scala.Tuple5;
import scala.collection.Seq;

import java.util.ArrayList;
import java.util.Arrays;
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
     * getNormFuncDesc.
     * @return
     */
    public static NormlizerFuncDescHelper getNormFuncDescHelper() {
        return NormlizerFuncDescHelper.getInstance();
    }

    /**
     * getScalerFuncDesc.
     * @return
     */
    public static ScalerFuncDescHelper getScalerFuncDescHelper() {
        return ScalerFuncDescHelper.getInstance();
    }

    /**
     * getBinarizerFuncDesc.
     * @return
     */
    public static BinarizerFuncDescHelper getBinarizerFuncDescHelper() {
        return BinarizerFuncDescHelper.getInstance();
    }

    /**
     * getBucketizerFuncDescHelper.
     * @return
     */
    public static BucketizerFuncDescHelper getBucketizerFuncDescHelper() {
        return BucketizerFuncDescHelper.getInstance();
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

    /**
     * Encapsulation of Norm Func Desc.
     */
    public static final class NormlizerFuncDescHelper {
        private static final NormlizerFuncDescHelper INSTANCE = new NormlizerFuncDescHelper();

        public MinerTool getNormFuncTool() {
            return MinerDesc.NormalizerFuncDesc$.MODULE$.getTool();
        }

        public MinerTool getNormFuncTool(boolean debug) {
            return MinerDesc.NormalizerFuncDesc$.MODULE$.getTool(debug);
        }

        public NormFuncParams needNormFuncToolParams() {
            return MinerDesc.NormalizerFuncDesc$.MODULE$.needParam();
        }

        private static NormlizerFuncDescHelper getInstance() {
            return INSTANCE;
        }

        private NormlizerFuncDescHelper() {}
    }

    /**
     * Encapsulation of Scaler Func Desc.
     */
    public static final class ScalerFuncDescHelper {
        private static final ScalerFuncDescHelper INSTANCE = new ScalerFuncDescHelper();

        public static final String STANDARD = "0";
        public static final String MIN_MAX = "1";
        public static final String MAX_ABS = "2";

        public MinerTool getStandardScalerFuncTool() {
            return MinerDesc.StandardScalerFuncDesc$.MODULE$.getTool();
        }

        public MinerTool getStandardScalerFuncTool(boolean debug) {
            return MinerDesc.StandardScalerFuncDesc$.MODULE$.getTool(debug);
        }

        public MinerTool getMinMaxScalerFuncTool() {
            return MinerDesc.MinMaxScalerFuncDesc$.MODULE$.getTool();
        }

        public MinerTool getMinMaxScalerFuncTool(boolean debug) {
            return MinerDesc.MinMaxScalerFuncDesc$.MODULE$.getTool(debug);
        }

        public MinerTool getMaxAbsScalerFuncTool() {
            return MinerDesc.MaxAbsScalerFuncDesc$.MODULE$.getTool();
        }

        public MinerTool getMaxAbsScalerFuncTool(boolean debug) {
            return MinerDesc.MaxAbsScalerFuncDesc$.MODULE$.getTool(debug);
        }

        public ScalerFuncParams needScalerFuncToolParams() {
            return MinerDesc.StandardScalerFuncDesc$.MODULE$.needParam();
        }

        public void setScalerParams(MinerTool tool, ScalerFuncParams params, String scalerType) {
            params.setScalerType(getScalerType(scalerType));
            tool.setParams(params);
        }

        private static ScalerFuncDescHelper getInstance() {
            return INSTANCE;
        }

        private Value getScalerType(String type) {
            if (type.equals(MIN_MAX)) {
                return ScalerType.MIN_MAX();
            } else if (type.equals(STANDARD)){
                return ScalerType.STANDARD();
            } else if (type.equals(MAX_ABS)) {
                return ScalerType.MAX_ABS();
            } else {
                List<String> mesList = new ArrayList();
                mesList.add(String.format("%s is not support.", type));

                throw new MinerException((Seq<String>) mesList);
            }
        }

        private ScalerFuncDescHelper() {}
    }

    /**
     * Encapsulation of Binarizer Func Desc.
     */
    public static final class BinarizerFuncDescHelper {
        private static final BinarizerFuncDescHelper INSTANCE = new BinarizerFuncDescHelper();

        public MinerTool getBinarizerFuncTool() {
            return MinerDesc.BinarizerFuncDesc$.MODULE$.getTool();
        }

        public MinerTool getBinarizerFuncTool(boolean debug) {
            return MinerDesc.BinarizerFuncDesc$.MODULE$.getTool(debug);
        }

        public BinarizerFuncParams needBinarizerFuncToolParams() {
            return MinerDesc.BinarizerFuncDesc$.MODULE$.needParam();
        }

        private static BinarizerFuncDescHelper getInstance() {
            return INSTANCE;
        }

        private BinarizerFuncDescHelper() {}
    }

    /**
     * Encapsulation of Lost Value Func Desc.
     */
    public static final class BucketizerFuncDescHelper {
        private static final BucketizerFuncDescHelper INSTANCE = new BucketizerFuncDescHelper();

        public MinerTool getBucketizerFuncTool() {
            return MinerDesc.BucketizerFuncDesc$.MODULE$.getTool();
        }

        public MinerTool getBucketizerFuncTool(boolean debug) {
            return MinerDesc.BucketizerFuncDesc$.MODULE$.getTool(debug);
        }

        public BucketizerFuncParams needBucketizerFuncToolParams() {
            return MinerDesc.BucketizerFuncDesc$.MODULE$.needParam();
        }

        /**
         * setBucketizerFuncToolParams.
         * @param tool
         * @param params
         * @param bucketizerFuncDefList List<String[]> :
         *         {[col_id, col_name, splits string split by ',', down bound, up bound]}
         *          col_id is number , its value is based on zero.
         */
        public void setBucketizerFuncToolParams(MinerTool tool,
                                                BucketizerFuncParams params,
                                                List<String[]> bucketizerFuncDefList) {
            if (bucketizerFuncDefList == null || bucketizerFuncDefList.size() <= 0) {
                return;
            }

            List<Tuple5<Integer, String, List<Double>, Boolean, Boolean>> paramsList
                    = new ArrayList<>(bucketizerFuncDefList.size());
            for (int i = 0; i < bucketizerFuncDefList.size(); i++) {
                String[] splitStrArr = bucketizerFuncDefList.get(i)[2].split(",");
                Arrays.sort(splitStrArr);
                List<Double> splits = Arrays.asList((Double[])ConvertUtils.convert(splitStrArr, Double.class));
                paramsList.add(new Tuple5(Integer.parseInt(bucketizerFuncDefList.get(i)[0]),
                        bucketizerFuncDefList.get(i)[1],
                        splits,
                        Boolean.valueOf(bucketizerFuncDefList.get(i)[3]),
                        Boolean.valueOf(bucketizerFuncDefList.get(i)[4])));
            }

            params.setJBuckFields(paramsList);
            tool.setParams(params);
        }

        private static BucketizerFuncDescHelper getInstance() {
            return INSTANCE;
        }
    }

    private JavaMinerDesc() {}
}
