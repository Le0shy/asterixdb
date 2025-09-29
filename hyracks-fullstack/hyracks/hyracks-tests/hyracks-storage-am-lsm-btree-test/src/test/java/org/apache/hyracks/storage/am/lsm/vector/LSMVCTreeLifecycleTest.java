package org.apache.hyracks.storage.am.lsm.vector;

import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import org.apache.hyracks.storage.am.common.AbstractIndexLifecycleTest;
import org.apache.hyracks.storage.am.common.CheckTuple;
import org.apache.hyracks.storage.am.common.IIndexTestContext;
import org.apache.hyracks.storage.am.common.TreeIndexTestUtils;
import org.apache.hyracks.storage.am.lsm.vector.impls.LSMVCTree;
import org.apache.hyracks.storage.am.lsm.vector.util.LSMVCTreeTestContext;
import org.apache.hyracks.storage.am.lsm.vector.util.LSMVCTreeTestHarness;
import org.apache.hyracks.storage.am.vector.VectorTreeTestUtils;

public class LSMVCTreeLifecycleTest extends AbstractIndexLifecycleTest {

    @SuppressWarnings("rawtypes")
    private final ISerializerDeserializer[] fieldSerdes = { IntegerSerializerDeserializer.INSTANCE,
            IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE,
            IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE };
//    private final IPrimitiveValueProviderFactory[] valueProviderFactories =
//            VectorTreeUtils.createPrimitiveValueProviderFactories(4, IntegerPointable.FACTORY);
    private final int numKeys = 4;

    private final LSMVCTreeTestHarness harness = new LSMVCTreeTestHarness();
    private final TreeIndexTestUtils titu = new VectorTreeTestUtils();

    @SuppressWarnings("rawtypes")
    private IIndexTestContext<? extends CheckTuple> testCtx;

    @Override
    protected boolean persistentStateExists() throws Exception {
        // make sure all of the directories exist
        if (!harness.getFileReference().getFile().exists()) {
            return false;
        }
        return true;
    }

    @Override
    protected boolean isEmptyIndex() throws Exception {
        return ((LSMVCTree) index).isEmptyIndex();
    }

    @Override
    public void setup() throws Exception {
        harness.setUp();
        testCtx = LSMVCTreeTestContext.create(harness.getIOManager(), harness.getVirtualBufferCaches(), harness.getFileReference(),
                harness.getDiskBufferCache(), fieldSerdes, 0, harness.getMergePolicy(), harness.getOperationTracker(),
                harness.getIOScheduler(), harness.getIOOperationCallbackFactory(), harness.getPageWriteCallbackFactory(),
                harness.getMetadataPageManagerFactory());
        index = testCtx.getIndex();
    }

    @Override
    public void tearDown() throws Exception {
        harness.tearDown();
    }

    @Override
    protected void performInsertions() throws Exception {
        titu.insertIntTuples(testCtx, 10, harness.getRandom());
    }

    @Override
    protected void checkInsertions() throws Exception {
        titu.checkScan(testCtx);
    }

    @Override
    protected void clearCheckableInsertions() throws Exception {
        testCtx.getCheckTuples().clear();
    }
}
