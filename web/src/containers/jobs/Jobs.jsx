// Copyright 2018 Red Hat, Inc
// Copyright 2025 Acme Gating, LLC
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
import { connect } from 'react-redux'
import { Link } from 'react-router-dom'
import {
  Checkbox,
  Badge,
  TreeView,
  TreeViewSearch,
  Toolbar,
  ToolbarContent,
  ToolbarItem,
} from '@patternfly/react-core'
import {
  CubeIcon,
} from '@patternfly/react-icons'

class JobsList extends React.Component {
  static propTypes = {
    tenant: PropTypes.object,
    jobs: PropTypes.array,
  }

  state = {
    filter: null,
    flatten: false,
  }

  handleKeyPress = (e) => {
    if (e.charCode === 13) {
      this.setState({filter: e.target.value})
      e.preventDefault()
      e.target.blur()
    }
  }

  onSearch = (e) => {
    this.setState({filter: e.target.value})
  }

  render () {
    const { jobs } = this.props
    const { filter, flatten } = this.state

    const linkPrefix = this.props.tenant.linkPrefix + '/job/'

    // job index map
    const jobMap = {}
    // nodes contains the tree data
    const nodes = []
    // visited contains individual node
    const visited = {}
    // createNode returns the actual node needed by the tree view component
    const createNode = (job, extra, jobId) => ({
      id: jobId,
      name: (
        <React.Fragment>
          <Link to={linkPrefix + encodeURIComponent(job.name)}>{job.name}</Link>
          {extra && (<span> ({extra})</span>)}
          {job.description && (
            <span style={{marginLeft: '10px'}}>{job.description}</span>
          )}
        </React.Fragment>),
      customBadgeContent: (
        <React.Fragment>
          {job.tags && job.tags.map((tag, idx) => (
            <Badge
              key={idx}>
              {tag}
            </Badge>))}
        </React.Fragment>),
      icon: <CubeIcon/>,
      hasBadge: job.tags,
      defaultExpanded: true,
    })
    // getNode returns the tree node and visit each parents
    const getNode = function (job, filtered) {
      if (!visited[job.name]) {
        // Collect parents
        let parents = []
        if (job.variants) {
          for (let jobVariant of job.variants) {
            if (jobVariant.parent &&
                jobVariant.parent !== job.name &&
                parents.indexOf(jobVariant.parent) === -1) {
              parents.push(jobVariant.parent)
            }
          }
        }
        visited[job.name] = createNode(job, null, job.name)
        visited[job.name].parents = parents
        visited[job.name].filtered = filtered
        // Visit parent recursively
        if (!flatten) {
          for (let parent of parents) {
            if (jobMap[parent]) {
              getNode(jobMap[parent], filtered)
            }
          }
        }
      }
      return visited[job.name]
    }
    // index job list
    for (let job of jobs) {
      jobMap[job.name] = job
    }
    // filter job
    let filtered = false
    if (filter) {
      filtered = true
      let filters = filter.replace(/ +/g, ',').split(',')
      for (let job of jobs) {
        filters.forEach(jobFilter => {
          if (jobFilter && (
            (job.name.indexOf(jobFilter) !== -1) ||
              (job.description && job.description.indexOf(jobFilter) !== -1))) {
            getNode(job, !filtered)
          }
        })
      }
    }
    // process job list
    for (let job of jobs) {
      const jobNode = getNode(job, filtered)
      if (!jobNode.filtered) {
        let attached = false
        if (!flatten) {
          // add tree node to each parent and expand the parent
          for (let parent of jobNode.parents) {
            const parentNode = visited[parent]
            if (!parentNode) {
              console.log(
                'Job ', job.name, ' parent ', parent, ' does not exist!')
              continue
            }
            if (!parentNode.children) {
              parentNode.children = []
            }
            if (attached) {
              // We need to create a duplicate node to satisfy TreeView constrains for multi parent
              parentNode.children.push(createNode(job, 'branched', parent.id + ' ' + job.name))
            } else {
              parentNode.children.push(jobNode)
            }
            attached = true
          }
        }
        // else add node at the tree root
        if (!attached || jobNode.parents.length === 0) {
          nodes.push(jobNode)
        }
      }
    }

    return (
      <div className="tree-view-container">
        <TreeView
          data={nodes}
          toolbar={
            <Toolbar>
              <ToolbarContent>
                <ToolbarItem>
                  <TreeViewSearch
                    onSearch={this.onSearch}
                  />
                </ToolbarItem>
                <ToolbarItem>
                  Flatten list &nbsp;
                  <Checkbox
                    id='flatten'
                    isChecked={flatten}
                    onChange={(e) => {this.setState({flatten: e})}} />
                </ToolbarItem>
              </ToolbarContent>
            </Toolbar>
          }/>
      </div>
    )
  }
}

export default connect(state => ({
  tenant: state.tenant,
}))(JobsList)
