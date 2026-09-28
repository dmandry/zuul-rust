// Copyright 2018 Red Hat, Inc
// Copyright 2020 BMW Group
// Copyright 2024 Acme Gating, LLC
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

import {
  AngleDoubleRightIcon,
  CheckIcon,
  ExclamationIcon,
  InfoIcon,
  InProgressIcon,
  PauseIcon,
  OutlinedClockIcon,
  TimesIcon,
} from '@patternfly/react-icons'

const QUEUE_ITEM_ICON_CONFIGS = {
  SUCCESS: {
    icon: CheckIcon,
    color: 'var(--zuul-color-success)',
    variant: 'success',
  },
  FAILURE: {
    icon: TimesIcon,
    color: 'var(--zuul-color-danger)',
    variant: 'danger',
  },
  MERGE_CONFLICT: {
    icon: ExclamationIcon,
    color: 'var(--zuul-color-warning)',
    variant: 'warning',
  },
  // TODO: identify a new color for merge error since it indicates
  // possible infrastructure failure.
  MERGE_ERROR: {
    icon: ExclamationIcon,
    color: 'var(--zuul-color-warning)',
    variant: 'warning',
  },
  QUEUED: {
    icon: OutlinedClockIcon,
    color: 'var(--zuul-color-info)',
    variant: 'info',
  },
  NON_LIVE: {
    icon: InfoIcon,
    color: 'var(--zuul-color-disabled)',
    variant: 'pending',
  },
}

const JOB_STATE_ICON_CONFIGS = {
  SUCCESS: {
    icon: CheckIcon,
    color: 'var(--zuul-color-success)',
    variant: 'success',
    labelColor: 'green',
  },
  FAILURE: {
    icon: TimesIcon,
    color: 'var(--zuul-color-danger)',
    variant: 'danger',
    labelColor: 'red',
  },
  LOST: {
    icon: TimesIcon,
    color: 'var(--zuul-color-danger)',
    variant: 'danger',
    labelColor: 'red',
  },
  PAUSED: {
    icon: PauseIcon,
    color: 'var(--zuul-color-info)',
    variant: 'info',
    labelColor: 'blue',
  },
  SNAPSHOTTING: {
    icon: PauseIcon,
    color: 'var(--zuul-color-info)',
    variant: 'info',
    labelColor: 'blue',
  },
  QUEUED: {
    icon: OutlinedClockIcon,
    color: 'var(--zuul-color-disabled)',
    variant: 'pending',
    labelColor: 'grey',
  },
  SKIPPED: {
    icon: AngleDoubleRightIcon,
    color: 'var(--zuul-color-info)',
    variant: 'info',
    labelColor: 'blue',
  },
  WAITING: {
    icon: OutlinedClockIcon,
    color: 'var(--zuul-color-disabled)',
    variant: 'pending',
    labelColor: 'grey',
  },
  CANCELED: {
    icon: TimesIcon,
    color: 'var(--zuul-color-disabled)',
    variant: 'pending',
    labelColor: 'grey',
  },
  POST_FAILURE: {
    icon: TimesIcon,
    color: 'var(--zuul-color-warning)',
    variant: 'warning',
    labelColor: 'orange',
  },
  NODE_FAILURE: {
    icon: TimesIcon,
    color: 'var(--zuul-color-warning)',
    variant: 'warning',
    labelColor: 'orange',
  },
  TIMED_OUT: {
    icon: TimesIcon,
    color: 'var(--zuul-color-danger)',
    variant: 'danger',
    labelColor: 'red',
  },
  RETRY_LIMIT: {
    icon: TimesIcon,
    color: 'var(--zuul-color-warning)',
    variant: 'warning',
    labelColor: 'orange',
  },
  UNSTABLE: {
    icon: TimesIcon,
    color: 'var(--zuul-color-warning)',
    variant: 'warning',
    labelColor: 'orange',
  },
  MERGE_CONFLICT: {
    icon: TimesIcon,
    color: 'var(--zuul-color-danger)',
    variant: 'danger',
    labelColor: 'red',
  },
  MERGE_ERROR: {
    icon: TimesIcon,
    color: 'var(--zuul-color-danger)',
    variant: 'danger',
    labelColor: 'red',
  },
}

const DEFAULT_JOB_STATE_ICON_CONFIG = {
  icon: InProgressIcon,
  color: 'var(--zuul-color-disabled)',
  variant: 'info',
  labelColor: 'grey',
}

const getJobResultIconConfig = (job) => {
  let iconConfig = DEFAULT_JOB_STATE_ICON_CONFIG
  let result = job.result ? job.result.toUpperCase() : null
  if (result !== null) {
    iconConfig = JOB_STATE_ICON_CONFIGS[result] || DEFAULT_JOB_STATE_ICON_CONFIG
  }
  return iconConfig
}

const getQueueItemIconConfig = (item) => {
  if (item.failing_reasons && item.failing_reasons.length > 0) {
    let reasons = item.failing_reasons.join(', ')
    if (reasons.match(/merge conflict/)) {
      return QUEUE_ITEM_ICON_CONFIGS['MERGE_CONFLICT']
    }
    if (reasons.match(/merge error/)) {
      return QUEUE_ITEM_ICON_CONFIGS['MERGE_ERROR']
    }
    return QUEUE_ITEM_ICON_CONFIGS['FAILURE']
  }

  if (item.active !== true) {
    return QUEUE_ITEM_ICON_CONFIGS['QUEUED']
  }

  if (item.live !== true) {
    return QUEUE_ITEM_ICON_CONFIGS['NON_LIVE']
  }

  return QUEUE_ITEM_ICON_CONFIGS['SUCCESS']
}

const getChangeLabel = (change) => {
  let changeId = change.id || 'NA'
  let changeTitle = changeId
  // Fall back to display the ref if there is no change id
  if (changeId === 'NA' && change.ref) {
    changeTitle = change.ref
  }
  let changeText = ''
  if (change.url !== null) {
    let githubId = changeId.match(/^([0-9]+),([0-9a-f]{40})$/)
    if (githubId) {
      changeTitle = githubId
      changeText = '#' + githubId[1]
    } else if (/^[0-9a-f]{40}$/.test(changeId)) {
      changeText = changeId.slice(0, 7)
    }
  } else if (changeId.length === 40) {
    changeText = changeId.slice(0, 7)
  }

  if (changeText !== '') {
    return changeText
  }
  return changeTitle
}

const getJobStrResult = (job) => {
  let result = job.result ? job.result.toLowerCase() : null
  if (result === null) {
    if (job.url === null) {
      if (job.queued === false) {
        result = 'waiting'
      } else {
        result = 'queued'
      }
    } else if (job.paused !== null && job.paused) {
      result = 'paused'
    } else if (job.snapshotting !== null && job.snapshotting) {
      result = 'snapshotting'
    } else {
      result = 'in progress'
    }
  }
  return result
}

const calculateQueueItemTimes = (item) => {
  let maxRemaining = 0
  let jobs = {}
  const now = Date.now()

  for (const job of item.jobs) {
    let jobElapsed = null
    let jobRemaining = null
    if (job.start_time) {
      let jobStart = parseInt(job.start_time * 1000)

      if (job.end_time) {
        let jobEnd = parseInt(job.end_time * 1000)
        jobElapsed = jobEnd - jobStart
      } else {
        jobElapsed = Math.max(now - jobStart, 0)
        if (job.estimated_time) {
          jobRemaining = Math.max(parseInt(job.estimated_time * 1000) - jobElapsed, 0)
        }
      }
    }
    if (jobRemaining && jobRemaining > maxRemaining) {
      maxRemaining = jobRemaining
    }
    jobs[job.name] = {
      elapsed: jobElapsed,
      remaining: jobRemaining,
    }
  }
  // If not all the jobs have started, this will be null, so only
  // use our value if it's oky to calculate it.
  if (item.remaining_time === null) {
    maxRemaining = null
  }
  return {
    remaining: maxRemaining,
    jobs: jobs,
  }
}

function getRefs(item) {
  // For backwards compat: get a list of this items refs.
  return 'refs' in item ? item.refs : [item]
}

function isPipelineEmpty(pipeline) {
  return (
    pipeline.change_queues
      .map(q => q.heads.flat().length)
      .reduce((a, len) => a + len, 0) === 0
  )
}

const countPipelineItems = (pipeline) => {
  let count = 0
  pipeline.change_queues = pipeline.change_queues.map(queue => {
    queue = { ...countQueueItems(queue) }
    count += queue._count
    return queue
  })
  pipeline._count = count
  return pipeline
}

const countQueueItems = (queue) => {
  let count = 0
  queue.heads.map(head => (
    head.map((item) => (
      item.live ? count++ : ''
    ))
  ))
  queue._count = count
  return queue
}

export {
  calculateQueueItemTimes,
  countQueueItems,
  countPipelineItems,
  getChangeLabel,
  getJobResultIconConfig,
  getJobStrResult,
  getQueueItemIconConfig,
  getRefs,
  isPipelineEmpty,
}
