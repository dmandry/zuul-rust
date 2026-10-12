// Copyright 2026 Acme Gating, LLC
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

import React, { useState } from 'react'
import { connect } from 'react-redux'
import PropTypes from 'prop-types'
import {
  ExpandableSection,
  ClipboardCopy,
} from '@patternfly/react-core'
import { fetchProjectSSHKey, fetchTenantSSHKey } from '../api'

function SSHKey(props) {
  const [isExpanded, setIsExpanded] = useState(false)
  const [contents, setContents] = useState()

  const onToggle = () => {
    setIsExpanded(!isExpanded)
  }

  if (isExpanded && !contents) {
    if (props.projectName) {
      fetchProjectSSHKey(props.tenant.apiPrefix, props.projectName)
        .then(response => setContents(response.data))
    } else {
      fetchTenantSSHKey(props.tenant.apiPrefix)
        .then(response => setContents(response.data))
    }
  }

  return (
    <ExpandableSection
      toggleText="Show public key"
      onToggle={onToggle}
      isExpanded={isExpanded}
    >
      <ClipboardCopy
        copyAriaLabel="Copy public key"
        hoverTip="Copy"
        clickTip="Copied"
        variant="inline-compact"
        isCode
      >{contents}
      </ClipboardCopy>
    </ExpandableSection>
    )
}

SSHKey.propTypes = {
  tenant: PropTypes.object,
  preferences: PropTypes.object,
  projectName: PropTypes.object,
}

function mapStateToProps(state) {
  return {
    tenant: state.tenant,
    preferences: state.preferences,
  }
}

export default connect(mapStateToProps)(SSHKey)
