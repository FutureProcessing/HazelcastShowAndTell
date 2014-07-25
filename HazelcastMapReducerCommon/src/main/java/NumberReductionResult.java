import java.io.Serializable;

public class NumberReductionResult implements Serializable {
        private int count;
        private int opResult;

        public NumberReductionResult(int count, int opResult) {
            this.count = count;
            this.opResult = opResult;
        }

        public int getCount() {
            return count;
        }

        public int getOpResult() {
            return opResult;
        }
}
