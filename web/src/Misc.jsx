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

import { useState, useEffect, React } from 'react'
import moment from 'moment'
import { ExternalLink } from './MiscComponents'

function removeHash() {
  // Remove location hash from url
  window.history.pushState('', document.title, window.location.pathname)
}

function buildExternalLink(ref) {
  /* TODO (felix): What should we show for periodic builds
      here? They don't provide a change, but the ref_url is
      also not usable */
  if (ref.ref_url && ref.change) {
    return (
      <ExternalLink target={ref.ref_url}>
        <strong>Change </strong>
        {ref.change},{ref.patchset}
      </ExternalLink>
    )
  } else if (ref.ref_url && ref.newrev) {
    return (
      <ExternalLink target={ref.ref_url}>
        <strong>Revision </strong>
        {ref.newrev.slice(0, 7)}
      </ExternalLink>
    )
  }

  return null
}

function buildExternalTableLink(ref) {
  /* TODO (felix): What should we show for periodic builds
      here? They don't provide a change, but the ref_url is
      also not usable */
  if (ref.ref_url && ref.change) {
    return (
      <ExternalLink target={ref.ref_url}>
        {ref.change},{ref.patchset}
      </ExternalLink>
    )
  } else if (ref.ref_url && ref.newrev) {
    return (
      <ExternalLink target={ref.ref_url}>
        {ref.newrev.slice(0, 7)}
      </ExternalLink>
    )
  }

  return null
}

function describeRef(ref) {
  if (ref.change) {
    return `Change ${ref.change}`
  } else {
    return `Ref ${ref.ref}`
  }
}

function renderRefInfo(ref) {
  const refinfo = ref.branch ? (
    <>
      <strong>Branch </strong> {ref.branch}
    </>
  ) : (
    <>
      <strong>Ref </strong> {ref.ref}
    </>
  )
  const oldrev = ref.oldrev ? (
    <><br /><strong>Old</strong> {ref.oldrev}</>
  ) : (<></>)
  const newrev = ref.newrev ? (
    <><br /><strong>New</strong> {ref.newrev}</>
  ) : (<></>)

  return (
    <>
      {refinfo}
      {oldrev}
      {newrev}
    </>
  )
}

function resolveDarkMode(theme) {
  let darkMode = false

  if (theme === 'Auto') {
    let matchMedia = window.matchMedia || function () {
      return {
        matches: false,
      }
    }

    darkMode = matchMedia('(prefers-color-scheme: dark)').matches
  } else if (theme === 'Dark') {
    darkMode = true
  }

  return darkMode
}

function setDarkMode(darkMode) {
  if (darkMode) {
    document.documentElement.classList.add('pf-theme-dark')
  } else {
    document.documentElement.classList.remove('pf-theme-dark')
  }
}

function formatTime(ms) {
  return moment.duration(ms).format({
    template: 'h [hr] m [min]',
    largest: 2,
    minValue: 1,
    usePlural: false,
  })
}

const NODE_STYLES = {
  requested: {
    variant: 'pending',
    color: '',
  },
  building: {
    variant: 'active',
    color: 'var(--zuul-color-active)',
  },
  ready: {
    variant: 'success',
    color: 'var(--pf-global--success-color--100)',
  },
  failed: {
    variant: 'danger',
    color: 'var(--pf-global--danger-color--100)',
  },
  tempfailed: {
    variant: 'danger',
    color: 'var(--pf-global--danger-color--100)',
  },
  'in-use': {
    variant: 'info',
    color: 'var(--pf-global--info-color--100)',
  },
  used: {
    variant: 'danger',
    color: 'var(--pf-global--danger-color--100)',
  },
  outdated: {
    variant: 'danger',
    color: 'var(--pf-global--danger-color--100)',
  },
  hold: {
    variant: 'info',
    color: 'var(--pf-global--info-color--100)',
  },
  unknown: {
    variant: 'pending',
    color: '',
  },
}

function getNodeStyle(node) {
  return NODE_STYLES[node.state] || NODE_STYLES['unknown']
}

function formatProviderName(providerName) {
  if (providerName !== null) {
    return providerName.split('/', 2)[1]
  } else {
    return providerName
  }
}

// from https://react.dev/reference/rules/components-and-hooks-must-be-pure#components-and-hooks-must-be-idempotent
function useTimeMinute() {
  const [time, setTime] = useState(() => new Date())

  useEffect(() => {
    const id = setInterval(() => {
      setTime(new Date())
    }, 1000 * 60)
    // For our purposes, we only need the time to update once per minute
    return () => clearInterval(id)
  }, [])

  return time
}

function isAuthorized(user, permission) {
  if (user.permissions && user.permissions['admin']) {
    return true
  }
  if (user.permissions && user.permissions[permission]) {
    return true
  }
  return false
}

export {
  buildExternalLink,
  buildExternalTableLink,
  describeRef,
  formatProviderName,
  formatTime,
  getNodeStyle,
  isAuthorized,
  removeHash,
  renderRefInfo,
  resolveDarkMode,
  setDarkMode,
  useTimeMinute,
}
