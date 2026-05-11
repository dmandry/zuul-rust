// Copyright 2018 Red Hat, Inc
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
import { JSONTree } from 'react-json-tree'
import {
  DescriptionList,
  DescriptionListTerm,
  DescriptionListGroup,
  DescriptionListDescription,
  Label,
  List,
  ListItem,
  ListVariant,
} from '@patternfly/react-core'
import {
  AnsibleTowerIcon,
  BanIcon,
  CatalogIcon,
  ClipboardCheckIcon,
  ClusterIcon,
  CodeBranchIcon,
  CodeIcon,
  ConnectedIcon,
  DisconnectedIcon,
  ExternalLinkAltIcon,
  FlagIcon,
  HistoryIcon,
  InfrastructureIcon,
  LockIcon,
  LockedIcon,
  OutlinedClockIcon,
  PackageIcon,
  RedoIcon,
  TopologyIcon,
  WrenchIcon
} from '@patternfly/react-icons'

import SourceContext from '../SourceContext'
import Nodeset from './Nodeset'
import Role from './Role'
import JobProject from './JobProject'
import JobIncludeExcludeProject from './JobIncludeExcludeProject'
import JobDescriptionCard from './JobDescriptionCard'


function FileMatchers({ fileMatchers }) {
  return (
    <span>
      <span style={{ fontFamily: 'RedHatMono' }}>
        {fileMatchers.regex}
      </span>
      {
        fileMatchers.negate
          ? <Label
            isCompact
            color="grey"
            style={{ marginLeft: 'var(--pf-global--spacer--xs)' }}
          >
            negated
          </Label>
          : ''
      }
    </span>
  )
}

FileMatchers.propTypes = {
  fileMatchers: PropTypes.object.isRequired,
}

class JobVariant extends React.Component {
  static propTypes = {
    parent: PropTypes.object,
    tenant: PropTypes.object,
    variant: PropTypes.object.isRequired,
    preferences: PropTypes.object,
  }

  renderStatus (variant) {
    const status = [{
      icon: variant.voting ? <ConnectedIcon /> : <DisconnectedIcon />,
      name: variant.voting ? 'Voting' : 'Non-voting'
    }]
    if (variant.abstract) {
      status.push({
        icon: <InfrastructureIcon />,
        name: 'Abstract'
      })
    }
    if (variant.final) {
      status.push({
        icon: <InfrastructureIcon />,
        name: 'Final'
      })
    }
    if (variant.post_review) {
      status.push({
        icon: <LockedIcon />,
        name: 'Post review'
      })
    }
    if (variant.protected) {
      status.push({
        icon: <LockedIcon />,
        name: 'Protected'
      })
    }
    if (variant.type === 'initializer') {
      status.push({
        icon: <TopologyIcon />,
        name: 'Initializer'
      })
    }
    if (variant.type === 'reporter') {
      status.push({
        icon: <TopologyIcon />,
        name: 'Reporter'
      })
    }

    return (
      <List iconSize="large" variant={ListVariant.inline}>
        {status.map((item, idx) => (
          <ListItem key={idx} icon={item.icon}>{item.name}</ListItem>
        ))}
      </List>
    )
  }

  render () {
    const { tenant, variant } = this.props
    const rows = []

    const jobInfos = [
      'source_context', 'builds', 'status',
      'parent', 'attempts', 'timeout', 'semaphores',
      'nodeset', 'nodeset_alternatives', 'variables',
      'override_checkout',
    ]
    jobInfos.forEach(key => {
      let label = key
      let nice_label = key
      let value = variant[key]

      if (label === 'source_context' && value) {
        value = (
          <SourceContext
            context={variant.source_context}
            showBranch={true}/>
        )
        nice_label = (<span><PackageIcon /> Defined at</span>)
      }
      if (label === 'builds') {
        value = (
          <Link to={this.props.tenant.linkPrefix + '/builds?job_name=' + variant.name}>
            <ExternalLinkAltIcon />&nbsp;{variant.name}
          </Link>
        )
        nice_label = (<span><HistoryIcon/> Build history</span>)
      }
      if (label === 'status') {
        value = this.renderStatus(variant)
        nice_label = (<span><FlagIcon/> Job flags</span>)
      }

      if (!value) {
        return
      }

      if (label === 'attempts') {
        nice_label = (<span><RedoIcon/> Retry attempts</span>)
      }

      if (label === 'timeout') {
        value = (<span>{value} seconds</span>)
        nice_label = (<span><OutlinedClockIcon /> Timeout</span>)
      }

      if (label === 'semaphores') {
        if (value.length === 0) {
          value = (<i>none</i>)
        } else {
          value = (
            <span style={{whiteSpace: 'pre-wrap'}}>
              <JSONTree
                data={value}
                hideRoot={true}
                sortObjectKeys={true}
                theme="default"
                invertTheme={!this.props.preferences.darkMode}
                shouldExpandNodeInitially={() => true}
              />
            </span>
          )
        }
        nice_label = (<span><LockIcon /> Semaphores</span>)
      }

      if (label === 'nodeset') {
        value = (
          <Nodeset nodeset={value} />
        )
        nice_label = (<span><ClusterIcon /> Required nodes</span>)
      }
      if (label === 'nodeset_alternatives') {
        value = value.map((alt, idx) => {
          return (<>
                    {(idx > 0 ? <span>or</span>:<></>)}
                    <Nodeset nodeset={alt} />
                  </>)
        })
        nice_label = (<span><ClusterIcon /> Required nodes</span>)
      }
      if (label === 'parent') {
        value = (
          <Link to={tenant.linkPrefix + '/job/' + value}>
            <ExternalLinkAltIcon />&nbsp;{value}
          </Link>
        )
        nice_label = (<span><CodeBranchIcon /> Parent</span>)
      }
      if (label === 'variables') {
        value = (
          <span style={{whiteSpace: 'pre-wrap'}}>
            <JSONTree
              data={value}
              hideRoot={true}
              sortObjectKeys={true}
              theme="default"
              invertTheme={!this.props.preferences.darkMode}
              shouldExpandNodeInitially={() => true}
            />
          </span>
        )
        nice_label = (<span><CodeIcon /> Job variables</span>)
      }

      if (label === 'description') {
        value = (
          <div style={{whiteSpace: 'pre-wrap'}}>
            {value}
          </div>
        )
        nice_label = (<span><CatalogIcon /> Description</span>)
      }

      rows.push({label: nice_label, value: value})

    })
    const jobInfosList = [
      'required_projects',
      'dependencies',
      'files',
      'irrelevant_files',
      'roles',
      'include_projects',
      'exclude_projects'
    ]
    jobInfosList.forEach(key => {
      let label = key
      let nice_label = key
      let values = variant[key]

      if (!values || values.length === 0) {
        return
      }
      const items = (
        <List isPlain>
          {values.map((value, idx) => {
            let item
            if (label === 'required_projects') {
              nice_label = 'Required Projects'
              item = <JobProject project={value} />
            } else if (label === 'include_projects') {
              nice_label = 'Include Projects'
              item = <JobIncludeExcludeProject project={value} />
            } else if (label === 'exclude_projects') {
              nice_label = 'Exclude Projects'
              item = <JobIncludeExcludeProject project={value} />
            } else if (label === 'roles') {
              nice_label = (<span><AnsibleTowerIcon /> Uses roles from</span>)
              item = <Role role={value} />
            } else if (label === 'dependencies') {
              nice_label = (<span><WrenchIcon /> Job dependencies</span>)
              if (value['soft']) {
                item = value['name'] + ' (soft)'
              } else {
                item = value['name']
              }
            } else if (label === 'irrelevant_files') {
              nice_label = (<span><BanIcon /> Irrelevant files matchers</span>)
              item = <FileMatchers fileMatchers={value} />
            } else if (label === 'files') {
              nice_label = (<span><ClipboardCheckIcon />Files matchers</span>)
              item = <FileMatchers fileMatchers={value} />
            } else {
              item = value
            }
            return (
              <ListItem key={idx} style={{margin:0}}>
                {item}
              </ListItem>
            )
          })}
        </List>
      )
      rows.push({label: nice_label, value: items})
    })
     return (
       <React.Fragment>
         <JobDescriptionCard description={variant.description}/>
         <DescriptionList isHorizontal
                          style={{'--pf-c-description-list--RowGap': '0.5rem'}}
                          className='pf-u-m-xl'>
          {rows.map((item, idx) => (
            <DescriptionListGroup key={idx}>
              <DescriptionListTerm>
                {item.label}
              </DescriptionListTerm>
              <DescriptionListDescription>
                {item.value}
              </DescriptionListDescription>
            </DescriptionListGroup>
          ))}
        </DescriptionList>
      </React.Fragment>
    )
  }
}

export default connect(state => ({
  tenant: state.tenant,
  preferences: state.preferences,
}))(JobVariant)
