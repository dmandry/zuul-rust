// Copyright 2018 Red Hat, Inc
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

import React from 'react'
import PropTypes from 'prop-types'


class JobIncludeExcludeProject extends React.Component {
  static propTypes = {
    project: PropTypes.object.isRequired
  }

  renderName() {
    const { project } = this.props
    return (
      <span>
        {project.name}
      </span>
    )
  }

  renderChange() {
    return (
      <span>Zuul change</span>
    )
  }

  renderItem() {
    return (
      <span>Item projects</span>
    )
  }

  render() {
    const { project } = this.props

    switch (project.type) {
    case "name":
      return this.renderName()
    case "change":
      return this.renderName()
    case "type":
      return this.renderName()
    default:
      return <span>Unknown</span>
    }
  }
}

export default JobIncludeExcludeProject
