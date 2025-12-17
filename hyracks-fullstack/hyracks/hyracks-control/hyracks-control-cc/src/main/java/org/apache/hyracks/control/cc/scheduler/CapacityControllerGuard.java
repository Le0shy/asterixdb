package org.apache.hyracks.control.cc.scheduler;

import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksException;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.api.job.resource.IClusterCapacity;
import org.apache.hyracks.api.job.resource.IJobCapacityController;
import org.apache.hyracks.control.cc.job.JobRun;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class CapacityControllerGuard {
    private static final Logger LOGGER = LogManager.getLogger();
    private final IJobCapacityController jobCapacityController;
    /* CPU Resources */
    private int CPUQuotaShort = 4;
    private int CPUQuotaCommon;
    private int CPUQuotaAvailableShort;
    private int CPUQuotaAvailableCommon;
    private int maximumAggregatedCores;

    /* Memory Resources */
    private double memoryPercentAllocatedToShort = 5.0;
    private long memoryAvailableShort;
    private long memoryAvailableCommon;
    private long maximumAggregatedMemoryByteSize;

    private boolean isUpdate;

    public CapacityControllerGuard(IJobCapacityController jobCapacityController) {
        this.jobCapacityController = jobCapacityController;
    }

    public IJobCapacityController.JobSubmissionStatus allocate(JobRun jobRun) throws HyracksException {
        if (!isUpdate) {
            update();
        }

        /* Short jobs */
        if (jobRun.getSchedulingType() == JobTypeManager.JobSchedulingType.SHORT) {
            return allocateResourceForShortJob(jobRun);
        }

        /* Other jobs */
        else {
            return allocateResourceForCommonJob(jobRun);
        }
    }

    private IJobCapacityController.JobSubmissionStatus allocateResourceForShortJob(JobRun jobRun)
            throws HyracksException {
        JobSpecification job = jobRun.getJobSpecification();
        IClusterCapacity requiredCapacity = job.getRequiredClusterCapacity();
        long reqAggregatedMemoryByteSize = requiredCapacity.getAggregatedMemoryByteSize();
        int reqAggregatedNumCores = requiredCapacity.getAggregatedCores();
        LOGGER.log(Level.DEBUG, "Job ID:{}: required aggregated memory byte size: {}, required aggregated cores:{}",
                jobRun.getJobId(), reqAggregatedMemoryByteSize, reqAggregatedNumCores);

        if (CPUQuotaShort < reqAggregatedNumCores || getMaximumMemoryByteSizeShort() < reqAggregatedMemoryByteSize) {
            throw HyracksException.create(ErrorCode.JOB_REQUIREMENTS_EXCEED_CAPACITY, requiredCapacity.toString(),
                    "CapacityControllerGuard for Short Job");
        }

        /* check cpu quota and memory available */
        if (CPUQuotaAvailableShort < reqAggregatedNumCores || memoryAvailableShort < reqAggregatedMemoryByteSize) {
            return IJobCapacityController.JobSubmissionStatus.QUEUE;
        }

        /* call capacityController's allocate */
        if (checkCapacityController(jobRun) == IJobCapacityController.JobSubmissionStatus.EXECUTE) {
            /* record resource usage for short jobs */
            memoryAvailableShort -= reqAggregatedMemoryByteSize;
            CPUQuotaAvailableShort -= reqAggregatedNumCores;
            logGuardInfo();
            return IJobCapacityController.JobSubmissionStatus.EXECUTE;
        } else {
            logGuardInfo();
            return IJobCapacityController.JobSubmissionStatus.QUEUE;
        }
    }

    private IJobCapacityController.JobSubmissionStatus allocateResourceForCommonJob(JobRun jobRun)
            throws HyracksException {
        JobSpecification job = jobRun.getJobSpecification();
        IClusterCapacity requiredCapacity = job.getRequiredClusterCapacity();
        long reqAggregatedMemoryByteSize = requiredCapacity.getAggregatedMemoryByteSize();
        int reqAggregatedNumCores = requiredCapacity.getAggregatedCores();

        if (CPUQuotaCommon < reqAggregatedNumCores || getMaximumMemoryByteSizeCommon() < reqAggregatedMemoryByteSize) {
            throw HyracksException.create(ErrorCode.JOB_REQUIREMENTS_EXCEED_CAPACITY, requiredCapacity.toString(),
                    "CapacityControllerGuard for Long Job");
        }

        /* check cpu quota and memory available */
        if (CPUQuotaAvailableCommon < requiredCapacity.getAggregatedCores()
                || memoryAvailableCommon < requiredCapacity.getAggregatedMemoryByteSize()) {
            return IJobCapacityController.JobSubmissionStatus.QUEUE;
        }

        /* then check resource manager */
        if (checkCapacityController(jobRun) == IJobCapacityController.JobSubmissionStatus.EXECUTE) {
            /* record resource usage for common jobs */
            memoryAvailableCommon -= reqAggregatedMemoryByteSize;
            CPUQuotaAvailableCommon -= reqAggregatedNumCores;
            return IJobCapacityController.JobSubmissionStatus.EXECUTE;
        } else {
            return IJobCapacityController.JobSubmissionStatus.QUEUE;
        }
    }

    private IJobCapacityController.JobSubmissionStatus checkCapacityController(JobRun jobRun) throws HyracksException {
        return jobCapacityController.allocate(jobRun.getJobSpecification(), jobRun.getJobId(), jobRun.getFlags());
    }

    public void release(JobRun jobRun) {
        IClusterCapacity requiredCapacity = jobRun.getJobSpecification().getRequiredClusterCapacity();
        long reqMemory = requiredCapacity.getAggregatedMemoryByteSize();
        int reqCores = requiredCapacity.getAggregatedCores();
        boolean isShort = jobRun.getSchedulingType() == JobTypeManager.JobSchedulingType.SHORT;

        if (isShort) {
            long maxMemShort = getMaximumMemoryByteSizeShort();
            // Calculate how much memory can actually be added to short pool
            long memoryToShort = Math.min(reqMemory, maxMemShort - memoryAvailableShort);
            memoryAvailableShort += memoryToShort;
            // Add the remainder to common pool
            memoryAvailableCommon += (reqMemory - memoryToShort);

            // Calculate how many cores can actually be added to short pool
            int coresToShort = Math.min(reqCores, CPUQuotaShort - CPUQuotaAvailableShort);
            CPUQuotaAvailableShort += coresToShort;
            // Add the remainder to common pool
            CPUQuotaAvailableCommon += (reqCores - coresToShort);
        } else {
            long maxMemCommon = getMaximumMemoryByteSizeCommon();
            // Calculate how much memory can actually be added to common pool
            long memoryToCommon = Math.min(reqMemory, maxMemCommon - memoryAvailableCommon);
            memoryAvailableCommon += memoryToCommon;
            // Add the remainder to short pool
            memoryAvailableShort += (reqMemory - memoryToCommon);

            // Calculate how many cores can actually be added to common pool
            int coresToCommon = Math.min(reqCores, CPUQuotaCommon - CPUQuotaAvailableCommon);
            CPUQuotaAvailableCommon += coresToCommon;
            // Add the remainder to short pool
            CPUQuotaAvailableShort += (reqCores - coresToCommon);
        }
        jobCapacityController.release(jobRun.getJobSpecification());
    }

    public void update() {
        isUpdate = true;
        /* available memory resources for short jobs and other jobs */
        maximumAggregatedMemoryByteSize = jobCapacityController.getMaxAggregatedMemoryByteSize();
        memoryAvailableShort = (long) (maximumAggregatedMemoryByteSize * (memoryPercentAllocatedToShort / 100.0));
        memoryAvailableCommon = maximumAggregatedMemoryByteSize - memoryAvailableShort;

        /* available cpu resources for short jobs and other jobs */
        maximumAggregatedCores = jobCapacityController.getMaxAggregatedNumCores();
        CPUQuotaAvailableShort = CPUQuotaShort;
        CPUQuotaCommon = maximumAggregatedCores - CPUQuotaShort;
        CPUQuotaAvailableCommon = maximumAggregatedCores - CPUQuotaAvailableShort;
        logGuardInfo();
    }

    public boolean isUpdate() {
        return isUpdate;
    }

    public double getMemoryRatio(JobSpecification jobSpecification) {
        return (double) jobSpecification.getRequiredClusterCapacity().getAggregatedMemoryByteSize()
                / maximumAggregatedMemoryByteSize;
    }

    public boolean isSQAResourcesAvailable() {
        /* TODO not necessary ? */
        return CPUQuotaShort > 3;
    }

    public long getMaximumMemoryByteSizeShort() {
        return (long) (maximumAggregatedMemoryByteSize * (memoryPercentAllocatedToShort / 100.0));
    }

    public long getMaximumMemoryByteSizeCommon() {
        return (long) (maximumAggregatedMemoryByteSize * (1 - memoryPercentAllocatedToShort / 100.0));
    }

    /*  TODO: dynamically adjust resources limits from different categories   */
    /*  CPU Quota
                     short       common
        limit:           6            6
        used:            4            0
        available:       2            6
        ->  set short to 3
        since 2 < 6 - 3 = 3
        limit:           3            9
        used:            4            0
        available:       0            8
        -> four used CPU Quota for short job is released
        available:
                         3            9
     */

    public void setCPUQuota(int newShortCPUQuota) {
        int diffQuotaLimit = newShortCPUQuota - CPUQuotaShort;
        /* Reducing short quota: move unused short quota to common
        Or:Increasing short quota: move from common to short */
        if (diffQuotaLimit < 0) {
            int reclaim = Math.min(CPUQuotaAvailableShort, -diffQuotaLimit);
            CPUQuotaAvailableShort -= reclaim;
            CPUQuotaAvailableCommon += reclaim;
        } else {
            int transfer = Math.min(CPUQuotaAvailableCommon, diffQuotaLimit);
            CPUQuotaAvailableShort += transfer;
            CPUQuotaAvailableCommon -= transfer;
        }

        CPUQuotaShort = newShortCPUQuota;
        CPUQuotaCommon = maximumAggregatedCores - CPUQuotaShort;
    }

    public void setMemoryPercentAllocatedToShort(double newShortMemoryPercent) {
        long diffMemoryBytesLimit = (long) (((newShortMemoryPercent - memoryPercentAllocatedToShort) / 100.0)
                * maximumAggregatedMemoryByteSize);

        if (diffMemoryBytesLimit < 0) {
            long reclaim = Math.min(memoryAvailableShort, -diffMemoryBytesLimit);
            memoryAvailableShort -= reclaim;
            memoryAvailableCommon += reclaim;
        } else {
            long transfer = Math.min(memoryAvailableCommon, diffMemoryBytesLimit);
            memoryAvailableShort += transfer;
            memoryAvailableCommon -= transfer;
        }

        memoryPercentAllocatedToShort = newShortMemoryPercent;
    }

    public void logGuardInfo() {
        LOGGER.log(Level.DEBUG,
                "Resource Info from WLM: "
                        + "CPU - Maximum Quota:{}, Short Job Quota:{}, Available Short Job Quota:{}, "
                        + "Available Common Job Quota:{}",
                maximumAggregatedCores, CPUQuotaShort, CPUQuotaAvailableShort, CPUQuotaAvailableCommon);
        LOGGER.log(Level.DEBUG,
                "Resource Info from WLM: "
                        + "Memory - Maximum Quota:{}, Short Job Quota:{}, Available Short Job Quota:{}, "
                        + "Available Common Job Quota:{}",
                maximumAggregatedMemoryByteSize, getMaximumMemoryByteSizeShort(), memoryAvailableShort,
                memoryAvailableCommon);
    }
}
