package org.apache.hyracks.control.cc.scheduler;

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

    public IJobCapacityController.JobSubmissionStatus allocate(JobRun jobRun) throws HyracksException {
        JobSpecification job = jobRun.getJobSpecification();

        /* Short jobs */
        if(jobRun.getSchedulingType() == JobTypeManager.JobSchedulingType.SHORT) {
            return allocateResourceForShortJob(job);
        }

        /* Other jobs */
        else {
            return allocateResourceForCommonJob(job);
        }
    }

    private IJobCapacityController.JobSubmissionStatus allocateResourceForShortJob(JobSpecification job)
            throws HyracksException {
        IClusterCapacity requiredCapacity = job.getRequiredClusterCapacity();
        long reqAggregatedMemoryByteSize = requiredCapacity.getAggregatedMemoryByteSize();
        int reqAggregatedNumCores = requiredCapacity.getAggregatedCores();

        /* check cpu quota and memory available */
        if (CPUQuotaShort < requiredCapacity.getAggregatedCores() ||
                memoryAvailableShort < requiredCapacity.getAggregatedMemoryByteSize()) {
            return IJobCapacityController.JobSubmissionStatus.QUEUE;
        }

        /* call capacityController's allocate */
        if (checkCapacityController(job)
                == IJobCapacityController.JobSubmissionStatus.EXECUTE) {
            /* record resource usage for short jobs */
            memoryAvailableShort -= reqAggregatedMemoryByteSize;
            CPUQuotaShort -= reqAggregatedNumCores;
            return IJobCapacityController.JobSubmissionStatus.EXECUTE;
        } else {
            return IJobCapacityController.JobSubmissionStatus.QUEUE;
        }
    }

    private IJobCapacityController.JobSubmissionStatus allocateResourceForCommonJob(JobSpecification job)
            throws HyracksException {
        IClusterCapacity requiredCapacity = job.getRequiredClusterCapacity();
        long reqAggregatedMemoryByteSize = requiredCapacity.getAggregatedMemoryByteSize();
        int reqAggregatedNumCores = requiredCapacity.getAggregatedCores();

        /* check cpu quota and memory available */
        if (CPUQuotaCommon < requiredCapacity.getAggregatedCores() ||
                memoryAvailableCommon < requiredCapacity.getAggregatedMemoryByteSize()) {
            return IJobCapacityController.JobSubmissionStatus.QUEUE;
        }

        /* then check resource manager */
        if (checkCapacityController(job)
                == IJobCapacityController.JobSubmissionStatus.EXECUTE) {
            /* record resource usage for common jobs */
            memoryAvailableCommon -= reqAggregatedMemoryByteSize;
            CPUQuotaCommon -= reqAggregatedNumCores;
            return IJobCapacityController.JobSubmissionStatus.EXECUTE;
        } else {
            return IJobCapacityController.JobSubmissionStatus.QUEUE;
        }
    }

    private IJobCapacityController.JobSubmissionStatus checkCapacityController(JobSpecification job)
            throws HyracksException {
        return jobCapacityController.allocate(job);
    }

    public void release(JobRun jobRun) {
        IClusterCapacity requiredCapacity = jobRun.getJobSpecification().getRequiredClusterCapacity();
        long reqAggregatedMemoryByteSize = requiredCapacity.getAggregatedMemoryByteSize();
        int reqAggregatedNumCores = requiredCapacity.getAggregatedCores();

        /* Short jobs */
        if(jobRun.getSchedulingType() == JobTypeManager.JobSchedulingType.SHORT) {
            memoryAvailableShort += reqAggregatedMemoryByteSize;
            CPUQuotaShort += reqAggregatedNumCores;
            jobCapacityController.release(jobRun.getJobSpecification());
        }

        /* Other jobs */
        else {
            memoryAvailableCommon += reqAggregatedMemoryByteSize;
            CPUQuotaCommon += reqAggregatedNumCores;
            jobCapacityController.release(jobRun.getJobSpecification());
        }

    }

    public double getMemoryRatio(JobSpecification jobSpecification) {
        return (double) jobSpecification.getRequiredClusterCapacity().getAggregatedMemoryByteSize()
                / maximumAggregatedMemoryByteSize;
    }

    public boolean isSQAResourcesAvailable() {
        return CPUQuotaShort >= 4;
    }
}
