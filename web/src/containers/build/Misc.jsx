// Copyright 2020 BMW Group
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

import * as React from 'react'
import PropTypes from 'prop-types'
import { Link } from 'react-router-dom'
import {
    Label,
} from '@patternfly/react-core'
import {
  CheckIcon,
  ExclamationIcon,
  QuestionIcon,
  TimesIcon,
} from '@patternfly/react-icons'
import { ConditionalWrapper } from '../../MiscComponents'

const RESULT_ICON_CONFIGS = {
  // In progress
  null: {
    icon: QuestionIcon,
    color: 'var(--pf-global--info-color--100)',
    badgeColor: 'blue',
  },
  SUCCESS: {
    icon: CheckIcon,
    color: 'var(--pf-global--success-color--100)',
    badgeColor: 'green',
  },
  FAILURE: {
    icon: TimesIcon,
    color: 'var(--pf-global--danger-color--100)',
    badgeColor: 'red',
  },
  RETRY_LIMIT: {
    icon: TimesIcon,
    color: 'var(--pf-global--danger-color--100)',
    badgeColor: 'red',
  },
  SKIPPED: {
    icon: QuestionIcon,
    color: 'var(--pf-global--info-color--100)',
    badgeColor: 'blue',
  },
  ABORTED: {
    icon: QuestionIcon,
    color: 'var(--pf-global--info-color--100)',
    badgeColor: 'yellow',
  },
  MERGE_CONFLICT: {
    icon: ExclamationIcon,
    color: 'var(--pf-global--warning-color--100)',
    badgeColor: 'orange',
  },
  MERGE_ERROR: {
    icon: ExclamationIcon,
    color: 'var(--pf-global--warning-color--100)',
    badgeColor: 'orange',
  },
  MERGE_FAILURE: {
    icon: ExclamationIcon,
    color: 'var(--pf-global--warning-color--100)',
    badgeColor: 'orange',
  },
  NODE_FAILURE: {
    icon: ExclamationIcon,
    color: 'var(--pf-global--warning-color--100)',
    badgeColor: 'orange',
  },
  TIMED_OUT: {
    icon: ExclamationIcon,
    color: 'var(--pf-global--warning-color--100)',
    badgeColor: 'orange',
  },
  POST_FAILURE: {
    icon: ExclamationIcon,
    color: 'var(--pf-global--warning-color--100)',
    badgeColor: 'orange',
  },
  CONFIG_ERROR: {
    icon: ExclamationIcon,
    color: 'var(--pf-global--warning-color--100)',
    badgeColor: 'orange',
  },
}

const DEFAULT_RESULT_ICON_CONFIG = {
  icon: ExclamationIcon,
  color: 'var(--pf-global--warning-color--100)',
  badgeColor: 'orange',
}

function BuildResult(props) {
  const { result, link = undefined, colored = true } = props
  const iconConfig = RESULT_ICON_CONFIGS[result] || DEFAULT_RESULT_ICON_CONFIG
  const color = colored ? iconConfig.color : 'inherit'

  return (
    <span style={{ color: color }}>
      <ConditionalWrapper
        condition={link}
        wrapper={children => <Link to={link} style={{ color: color }}>{children}</Link>}
      >
        {result || 'In Progress'}
      </ConditionalWrapper>
    </span>
  )
}

BuildResult.propTypes = {
  result: PropTypes.string,
  link: PropTypes.string,
  colored: PropTypes.bool,
}

function BuildResultBadge(props) {
  const { result } = props
  const iconConfig = RESULT_ICON_CONFIGS[result] || DEFAULT_RESULT_ICON_CONFIG
  const color = iconConfig.badgeColor

  return (
    <Label
      color={color}
      style={{
        marginLeft: 'var(--pf-global--spacer--sm)',
        verticalAlign: '0.15em',
      }}
    >
      {result || 'In Progress'}
    </Label>
  )
}

BuildResultBadge.propTypes = {
  result: PropTypes.string,
}

function BuildResultWithIcon(props) {
  const { result, link = undefined, colored = true, size = 'sm' } = props
  const iconConfig = RESULT_ICON_CONFIGS[result] || DEFAULT_RESULT_ICON_CONFIG

  // Define the verticalAlign based on the size
  let verticalAlign = '-0.2em'

  if (size === 'md') {
    verticalAlign = '-0.35em'
  }

  const Icon = iconConfig.icon
  const color = colored ? iconConfig.color : 'inherit'

  return (
    <span style={{ color: color }}>
      <ConditionalWrapper
        condition={link}
        wrapper={children => <Link to={link} style={{ color: color }}>{children}</Link>}
      >
        <Icon
          size={size}
          style={{
            marginRight: 'var(--pf-global--spacer--sm)',
            verticalAlign: verticalAlign,
          }}
        />
        {props.children}
      </ConditionalWrapper>
    </span>
  )
}

BuildResultWithIcon.propTypes = {
  result: PropTypes.string,
  link: PropTypes.string,
  colored: PropTypes.bool,
  size: PropTypes.string,
  children: PropTypes.node,
}



export { BuildResult, BuildResultBadge, BuildResultWithIcon }
