package org.apache.asterix.runtime.job.resource;

import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksException;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.api.job.resource.IClusterCapacity;
import org.apache.hyracks.api.job.resource.IJobCapacityController;
import org.apache.hyracks.api.job.resource.IReadOnlyClusterCapacity;
import org.apache.hyracks.control.cc.job.JobRun;
import org.apache.hyracks.control.cc.scheduler.IResourceManager;
import org.apache.hyracks.control.cc.scheduler.JobTypeManager;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class WorkloadCapacityController {
    private static final Logger LOGGER = LogManager.getLogger();
    private final IResourceManager resourceManager;
    private final int shortJobCPUQuota = 1;
    private final int commonJobCPUQuota = 2;
    private final double memeoryAllocatedToShort = 0.1;
    private final long maximumAggregatedMemoryByteSize;
    private final long maximumMemoryShort;
    private long availableMemoryShort;
    private final long maximumMemoryCommon;
    private long availableMemoryCommon;
    public WorkloadCapacityController(IResourceManager resourceManager) {
        this.resourceManager = resourceManager;
        this.maximumAggregatedMemoryByteSize = resourceManager.getMaximumCapacity().getAggregatedMemoryByteSize();
        maximumMemoryShort = (long)(maximumAggregatedMemoryByteSize * memeoryAllocatedToShort);
        maximumMemoryCommon = maximumAggregatedMemoryByteSize - maximumMemoryShort;

        availableMemoryShort = maximumMemoryShort;
        availableMemoryCommon = maximumMemoryCommon;
    }

    public IJobCapacityController.JobSubmissionStatus allocate(JobRun jobRun) throws HyracksException {
        if(jobRun.getSchedulingType() == JobTypeManager.JobSchedulingType.SHORT) {
            if (shortJobCPUQuota == 0) {
                return IJobCapacityController.JobSubmissionStatus.QUEUE;
            }
            return allocateShort(jobRun.getJobSpecification());
        } else {
            if (commonJobCPUQuota == 0) {
                return IJobCapacityController.JobSubmissionStatus.QUEUE;
            }
            return allocateCommon(jobRun.getJobSpecification());
        }
    }

    public IJobCapacityController.JobSubmissionStatus allocateShort(JobSpecification job) throws HyracksException {
        IClusterCapacity requiredCapacity = job.getRequiredClusterCapacity();
        long reqAggregatedMemoryByteSize = requiredCapacity.getAggregatedMemoryByteSize();
        int reqAggregatedNumCores = requiredCapacity.getAggregatedCores();
        IReadOnlyClusterCapacity maximumCapacity = resourceManager.getMaximumCapacity();
        if (!(reqAggregatedMemoryByteSize <= maximumMemoryShort
                && reqAggregatedNumCores <= maximumCapacity.getAggregatedCores())) {
            throw HyracksException.create(ErrorCode.JOB_REQUIREMENTS_EXCEED_CAPACITY, requiredCapacity.toString(),
                    maximumCapacity.toString());
        }
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

    public IJobCapacityController.JobSubmissionStatus allocateCommon(JobSpecification job) throws HyracksException {
        IClusterCapacity requiredCapacity = job.getRequiredClusterCapacity();
        long reqAggregatedMemoryByteSize = requiredCapacity.getAggregatedMemoryByteSize();
        int reqAggregatedNumCores = requiredCapacity.getAggregatedCores();
        IReadOnlyClusterCapacity maximumCapacity = resourceManager.getMaximumCapacity();
        if (!(reqAggregatedMemoryByteSize <= maximumMemoryShort
                && reqAggregatedNumCores <= maximumCapacity.getAggregatedCores())) {
            throw HyracksException.create(ErrorCode.JOB_REQUIREMENTS_EXCEED_CAPACITY, requiredCapacity.toString(),
                    maximumCapacity.toString());
        }
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

    public void release(JobSpecification job) {

    }

    public double getMemoryRatio(JobSpecification jobSpecification) {
        return (double) jobSpecification.getRequiredClusterCapacity().getAggregatedMemoryByteSize()
                / resourceManager.getMaximumCapacity().getAggregatedMemoryByteSize();
    }
}
