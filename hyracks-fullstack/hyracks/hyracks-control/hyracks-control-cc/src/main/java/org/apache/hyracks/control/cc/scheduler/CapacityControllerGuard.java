package org.apache.hyracks.control.cc.scheduler;

import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksException;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.api.job.resource.IClusterCapacity;
import org.apache.hyracks.api.job.resource.IJobCapacityController;
import org.apache.hyracks.api.job.resource.IReadOnlyClusterCapacity;
import org.apache.hyracks.control.cc.job.JobRun;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class CapacityControllerGuard {
    private static final Logger LOGGER = LogManager.getLogger();
    private final IJobCapacityController jobCapacityController;
    private int CPUQuotaShort = 8;
    private int CPUQuotaCommon;
    private final double memoryAllocatedToShort = 0.1;

    private final int maximumAggregatedCores;

    private final long maximumAggregatedMemoryByteSize;
    //private final long maximumMemoryShort;
    private long memoryAvailableShort;
    //private final long maximumMemoryCommon;
    private long memoryAvailableCommon;

    public CapacityControllerGuard(IJobCapacityController jobCapacityController) {
        this.jobCapacityController = jobCapacityController;

        /* available memory resources for short jobs and other jobs */
        maximumAggregatedMemoryByteSize = jobCapacityController.getAggregatedMemeoryByteSize();
        memoryAvailableShort = (long)(maximumAggregatedMemoryByteSize * memoryAllocatedToShort);
        memoryAvailableCommon = maximumAggregatedMemoryByteSize - memoryAvailableShort;

        /* available cpu resources for short jobs and other jobs */
        maximumAggregatedCores = jobCapacityController.getAggregatedNumCores();
        CPUQuotaCommon = maximumAggregatedCores - CPUQuotaShort;
    }

    private void checkIfExceedMaximumCapacity(IReadOnlyClusterCapacity requiredCapacity) throws HyracksException {
        long reqAggregatedMemoryByteSize = requiredCapacity.getAggregatedMemoryByteSize();
        int reqAggregatedNumCores = requiredCapacity.getAggregatedCores();

        if (!(reqAggregatedMemoryByteSize <= maximumAggregatedMemoryByteSize
                && reqAggregatedNumCores <= maximumCapacity.getAggregatedCores())) {
            throw HyracksException.create(ErrorCode.JOB_REQUIREMENTS_EXCEED_CAPACITY, requiredCapacity.toString(),
                    maximumCapacity.toString());
        }
    }
    public IJobCapacityController.JobSubmissionStatus allocate(JobRun jobRun) throws HyracksException {
        JobSpecification job = jobRun.getJobSpecification();
        IReadOnlyClusterCapacity requiredCapacity = job.getRequiredClusterCapacity();
        /* check if the required capacity exceeds maximum */
        checkIfExceedMaximumCapacity(requiredCapacity);

        /* Short jobs */
        if(jobRun.getSchedulingType() == JobTypeManager.JobSchedulingType.SHORT) {
            /* check cpu quota and memory available */
            if (CPUQuotaShort < requiredCapacity.getAggregatedCores() ||
                    memoryAvailableShort < requiredCapacity.getAggregatedMemoryByteSize()) {
                return IJobCapacityController.JobSubmissionStatus.QUEUE;
            }
            /* then check resource manager */
            if (checkResourceManager(jobRun.getJobSpecification().getRequiredClusterCapacity())
                    == IJobCapacityController.JobSubmissionStatus.EXECUTE) {
                /* record resource usage for short jobs */
                allocateResourceForShortJob(requiredCapacity.getAggregatedMemoryByteSize(),
                        requiredCapacity.getAggregatedCores());
                return IJobCapacityController.JobSubmissionStatus.EXECUTE;
            } else {
                return IJobCapacityController.JobSubmissionStatus.QUEUE;
            }
        }
        /* Other jobs */
        else {
            if (CPUQuotaCommon < requiredCapacity.getAggregatedCores() ||
                    memoryAvailableCommon < requiredCapacity.getAggregatedMemoryByteSize()) {
                return IJobCapacityController.JobSubmissionStatus.QUEUE;
            }
            if (checkResourceManager(jobRun.getJobSpecification().getRequiredClusterCapacity())
                    == IJobCapacityController.JobSubmissionStatus.EXECUTE) {
                /* record resource usage for common jobs */
                allocateResourceForCommonJob(requiredCapacity.getAggregatedMemoryByteSize(),
                        requiredCapacity.getAggregatedCores());
                return IJobCapacityController.JobSubmissionStatus.EXECUTE;
            } else {
                return IJobCapacityController.JobSubmissionStatus.QUEUE;
            }
        }
    }

    public IJobCapacityController.JobSubmissionStatus checkResourceManager(IClusterCapacity requiredCapacity) throws HyracksException {
        long reqAggregatedMemoryByteSize = requiredCapacity.getAggregatedMemoryByteSize();
        int reqAggregatedNumCores = requiredCapacity.getAggregatedCores();

        IClusterCapacity currentCapacity = resourceManager.getCurrentCapacity();
        long currentAggregatedMemoryByteSize = currentCapacity.getAggregatedMemoryByteSize();
        int currentAggregatedAvailableCores = currentCapacity.getAggregatedCores();

        if (!(reqAggregatedMemoryByteSize <= currentAggregatedMemoryByteSize
                && reqAggregatedNumCores <= currentAggregatedAvailableCores)) {
            return IJobCapacityController.JobSubmissionStatus.QUEUE;
        }
        currentCapacity.setAggregatedMemoryByteSize(currentAggregatedMemoryByteSize - reqAggregatedMemoryByteSize);
        currentCapacity.setAggregatedCores(currentAggregatedAvailableCores - reqAggregatedNumCores);
        return IJobCapacityController.JobSubmissionStatus.EXECUTE;
    }

    private void allocateResourceForShortJob(long reqAggregatedMemoryByteSize, int reqAggregatedNumCores) {
        memoryAvailableShort -= reqAggregatedMemoryByteSize;
        CPUQuotaShort -= reqAggregatedNumCores;
    }

    private void allocateResourceForCommonJob(long reqAggregatedMemoryByteSize, int reqAggregatedNumCores) {
        memoryAvailableCommon -= reqAggregatedMemoryByteSize;
        CPUQuotaCommon -= reqAggregatedNumCores;
    }

    public void release(JobSpecification job) {
        IClusterCapacity requiredCapacity = job.getRequiredClusterCapacity();
        long reqAggregatedMemoryByteSize = requiredCapacity.getAggregatedMemoryByteSize();
        int reqAggregatedNumCores = requiredCapacity.getAggregatedCores();
        IClusterCapacity currentCapacity = resourceManager.getCurrentCapacity();
        long aggregatedMemoryByteSize = currentCapacity.getAggregatedMemoryByteSize();
        int aggregatedNumCores = currentCapacity.getAggregatedCores();
        currentCapacity.setAggregatedMemoryByteSize(aggregatedMemoryByteSize + reqAggregatedMemoryByteSize);
        currentCapacity.setAggregatedCores(aggregatedNumCores + reqAggregatedNumCores);
        ensureMaxCapacity();
    }

    private void ensureMaxCapacity() {
        final IClusterCapacity currentCapacity = resourceManager.getCurrentCapacity();
        final IReadOnlyClusterCapacity maximumCapacity = resourceManager.getMaximumCapacity();
        if (currentCapacity.getAggregatedCores() > maximumCapacity.getAggregatedCores()
                || currentCapacity.getAggregatedMemoryByteSize() > maximumCapacity.getAggregatedMemoryByteSize()) {
            LOGGER.warn("Current cluster available capacity {} is more than its maximum capacity {}", currentCapacity,
                    maximumCapacity);
        }
    }

    public double getMemoryRatio(JobSpecification jobSpecification) {
        return (double) jobSpecification.getRequiredClusterCapacity().getAggregatedMemoryByteSize()
                / maximumAggregatedMemoryByteSize;
    }
}
