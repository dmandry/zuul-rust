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
import { isEqual } from 'lodash'
import PropTypes from 'prop-types'
import { connect } from 'react-redux'
import 'moment-duration-format'
import { PageSection, PageSectionVariants, Pagination } from '@patternfly/react-core'

import { fetchBuilds } from '../api'
import {
  getFiltersFromUrl,
  writeFiltersToUrl,
} from '../containers/FilterToolbar'
import {
  FilterToolbar,
} from '../containers/FilterToolbarComponents'
import { makeBuildQueryString } from '../containers/BuildQuery'
import BuildTable from '../containers/build/BuildTable'

class BuildsPageComponent extends React.Component {
  static propTypes = {
    tenant: PropTypes.object,
    timezone: PropTypes.string,
    location: PropTypes.object,
    history: PropTypes.object,
  }

  constructor(props) {
    super()
    this.filterCategories = [
      {
        key: 'job_name',
        title: 'Job',
        placeholder: 'Filter by Job...',
        type: 'search',
        fuzzy: true,
      },
      {
        key: 'project',
        title: 'Project',
        placeholder: 'Filter by Project...',
        type: 'search',
        fuzzy: true,
      },
      {
        key: 'branch',
        title: 'Branch',
        placeholder: 'Filter by Branch...',
        type: 'search',
        fuzzy: true,
      },
      {
        key: 'pipeline',
        title: 'Pipeline',
        placeholder: 'Filter by Pipeline...',
        type: 'search',
        fuzzy: true,
      },
      {
        key: 'change',
        title: 'Change',
        placeholder: 'Filter by Change...',
        type: 'search',
        fuzzy: false,
      },
      {
        key: 'result',
        title: 'Result',
        placeholder: 'Any result',
        type: 'select',
        // TODO there should be a single source of truth for this
        options: [
          'SUCCESS',
          'FAILURE',
          'RETRY_LIMIT',
          'POST_FAILURE',
          'SKIPPED',
          'NODE_FAILURE',
          'MERGE_CONFLICT',
          'MERGE_FAILURE',
          'CONFIG_ERROR',
          'TIMED_OUT',
          'CANCELED',
          'ERROR',
          'RETRY',
          'DISK_FULL',
          'NO_JOBS',
          'DISCONNECT',
          'ABORTED',
          'LOST',
          'EXCEPTION',
          'NO_HANDLE',
          'SNAPSHOT_FAILURE',
        ],
        fuzzy: false,
      },
      {
        key: 'uuid',
        title: 'Build',
        placeholder: 'Filter by Build UUID...',
        type: 'search',
        fuzzy: false,
      },
      {
        key: 'held',
        title: 'Held',
        placeholder: 'Choose Hold Status...',
        type: 'ternary',
        options: [
          'All',
          'Held Builds Only',
          'Non Held Builds Only',
        ],
        fuzzy: false,
      },
      {
        key: 'voting',
        title: 'Voting',
        placeholder: 'Choose Voting Status...',
        type: 'ternary',
        options: [
          'All',
          'Voting Only',
          'Non-Voting Only',
        ],
        fuzzy: false,
      },
    ]

    const _filters = getFiltersFromUrl(props.location, this.filterCategories)
    const perPage = _filters.limit[0]
      ? parseInt(_filters.limit[0])
      : 50
    const currentPage = _filters.skip[0]
      ? Math.floor(parseInt(_filters.skip[0] / perPage)) + 1
      : 1

    this.state = {
      builds: [],
      fetching: false,
      filters: _filters,
      resultsPerPage: perPage,
      currentPage: currentPage,
      itemCount: null,
    }
  }

  updateData = (filters) => {
    // When building the filter query for the API we can't rely on the location
    // search parameters. Although, we've updated them in theu URL directly
    // they always have the same value in here (the values when the page was
    // first loaded). Most probably that's the case because the location is
    // passed as prop and doesn't change since the page itself wasn't
    // re-rendered.
    const { itemCount } = this.state
    let paginationOptions = {
      skip: filters.skip.length > 0 ? filters.skip : [0,],
      limit: filters.limit.length > 0 ? filters.limit : [50,]
    }
    let _filters = { ...filters, ...paginationOptions }
    const queryString = makeBuildQueryString(_filters)
    this.setState({ fetching: true })
    // TODO (felix): What happens in case of a broken network connection? Is the
    // fetching shows infinitely or can we catch this and show an erro state in
    // the table instead?
    fetchBuilds(this.props.tenant.apiPrefix, queryString).then((response) => {
      // if we have already an itemCount for this query (ie we're scrolling backwards through results)
      // keep this value. Otherwise, check if we've got all the results.
      let finalItemCount = itemCount
        ? itemCount
        : (response.data.length < paginationOptions.limit[0]
          ? parseInt(paginationOptions.skip[0]) + response.data.length
          : null)
      this.setState({
        builds: response.data,
        fetching: false,
        itemCount: finalItemCount,
      })
    })
  }

  componentDidMount() {
    document.title = 'Zuul Builds'
    if (this.props.tenant.name) {
      this.updateData(this.state.filters)
    }
  }

  componentDidUpdate(prevProps) {
    const { filters } = this.state
    if (
      this.props.tenant.name !== prevProps.tenant.name ||
      this.props.timezone !== prevProps.timezone
    ) {
      this.updateData(filters)
    }
  }

  filterInputValidation = (filterKey, filterValue) => {
    // Input value should not be empty for all cases
    if (!filterValue) {
      return {
        success: false,
        message: 'Input should not be empty'
      }
    }

    // For change filter, it must be an integer
    if (filterKey === 'change' && isNaN(filterValue)) {
      return {
        success: false,
        message: 'Change must be an integer (do not include revision)'
      }
    }

    return {
      success: true
    }
  }

  handleFilterChange = (newFilters) => {
    const { location, history } = this.props
    const { filters, itemCount } = this.state
    /*eslint-disable-next-line*/
    let { 'skip': x1, 'limit': y1, ..._oldFilters } = filters
    /*eslint-disable-next-line*/
    let { 'skip': x2, 'limit': y2, ..._newFilters } = newFilters

    // If filters have changed, reinitialize skip
    let equalTest = isEqual(_oldFilters, _newFilters)
    let finalFilters = equalTest ? newFilters : { ...newFilters, skip: [0,] }

    // We must update the URL parameters before the state. Otherwise, the URL
    // will always be one filter selection behind the state. But as the URL
    // reflects our state this should be ok.
    writeFiltersToUrl(finalFilters, this.filterCategories, location, history)
    let newState = {
      filters: finalFilters,
      // if filters haven't changed besides skip or limit, keep our itemCount and currentPage
      itemCount: equalTest ? itemCount : null,
    }
    if (!equalTest) {
      newState.currentPage = 1
    }
    this.setState(
      newState,
      () => {
        this.updateData(finalFilters)
      })
  }

  handleClearFilters = () => {
    // Delete the values for each filter category
    const filters = this.filterCategories.reduce((filterDict, category) => {
      filterDict[category.key] = []
      return filterDict
    }, {})
    this.handleFilterChange(filters)
  }

  handlePerPageSelect = (event, perPage) => {
    const { filters } = this.state
    this.setState({ resultsPerPage: perPage })
    const newFilters = { ...filters, limit: [perPage,] }
    this.handleFilterChange(newFilters)
  }

  handleSetPage = (event, pageNumber) => {
    const { filters, resultsPerPage } = this.state
    this.setState({ currentPage: pageNumber })
    let offset = resultsPerPage * (pageNumber - 1)
    const newFilters = { ...filters, skip: [offset,] }
    this.handleFilterChange(newFilters)
  }


  render() {
    const { history } = this.props
    const { builds, fetching, filters, resultsPerPage, currentPage, itemCount } = this.state

    return (
      <PageSection variant={PageSectionVariants.light}>
        <FilterToolbar
          filterCategories={this.filterCategories}
          onFilterChange={this.handleFilterChange}
          filters={filters}
          filterInputValidation={this.filterInputValidation}
        />
        <Pagination
          toggleTemplate={({ firstIndex, lastIndex, itemCount }) => (
            <React.Fragment>
              <b>
                {firstIndex} - {lastIndex}
              </b>
              &nbsp;
              of
              &nbsp;
              <b>{itemCount ? itemCount : 'many'}</b>
            </React.Fragment>
          )}
          itemCount={itemCount}
          perPage={resultsPerPage}
          page={currentPage}
          widgetId="pagination-menu"
          onPerPageSelect={this.handlePerPageSelect}
          onSetPage={this.handleSetPage}
          isCompact
        />
        <BuildTable
          builds={builds}
          fetching={fetching}
          onClearFilters={this.handleClearFilters}
          history={history}
        />
      </PageSection>
    )
  }
}

const BuildsPage = connect((state) => ({
  tenant: state.tenant,
  timezone: state.timezone,
}))(BuildsPageComponent)
export default BuildsPage
