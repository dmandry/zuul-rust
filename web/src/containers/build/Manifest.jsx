// Copyright 2019 Red Hat, Inc
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
import {
  TreeView,
} from '@patternfly/react-core'
import { Link } from 'react-router-dom'

import { renderTree } from '../../actions/build'


class Manifest extends React.Component {
  static propTypes = {
    tenant: PropTypes.object.isRequired,
    build: PropTypes.object.isRequired,
    manifest: PropTypes.object.isRequired
  }

  render() {
    const { tenant, build, manifest } = this.props

    const raw_suffix = (obj) => {
      return (obj.mimetype === 'application/directory' &&
              manifest.index_links) ? 'index.html' : ''
    }

    const nodes = manifest.tree.map(
      n => renderTree(
        tenant, build, '/', n,
        (tenant, build, path, name, log_url, obj) => (
          <span>
            <Link
              to={tenant.linkPrefix + '/build/' + build.uuid + '/log' + path + name}>
              {obj.name}
            </Link>
            &nbsp;&nbsp;(<a href={log_url + path + name + raw_suffix(obj)}>raw</a>
            &nbsp;<span className="fa fa-external-link"/>)
          </span>),
        (log_url, path, name, obj) => (
          <span>
            {obj.name}
            &nbsp;&nbsp;(<a href={log_url + path + name + raw_suffix(obj)}>raw</a>
            &nbsp;<span className="fa fa-external-link"/>)
          </span>
        )))

    return (
      <React.Fragment>
        <br/>
        <div className="tree-view-container">
          <TreeView data={nodes} />
        </div>
      </React.Fragment>
    )
  }
}

export default Manifest
