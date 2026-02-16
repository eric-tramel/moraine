<script lang="ts">
  import { createEventDispatcher } from 'svelte';
  import type { TableDetailResponse } from '../types/api';
  import type { TableOption } from '../types/ui';
  import { isHttpUrl, searchQueryUrl, stringifyCellValue } from '../utils/url';

  export let tableTitle = 'Table';
  export let tableOptions: TableOption[] = [];
  export let selectedTable: string | null = null;
  export let rowLimit = 25;
  export let rowLimitOptions: number[] = [10, 25, 50, 100];
  export let schemaText = 'No schema metadata returned';
  export let schemaMuted = true;
  export let detail: TableDetailResponse | null = null;

  let selectedTableValue = '';
  let rowLimitValue = 25;

  const dispatch = createEventDispatcher<{
    tableChange: string;
    rowLimitChange: number;
  }>();

  $: columns = detail?.schema?.map((column) => column.name) ?? [];
  $: rows = detail?.rows ?? [];
  $: selectedTableValue = selectedTable || '';
  $: rowLimitValue = rowLimit;

  function handleTableChange(): void {
    dispatch('tableChange', selectedTableValue);
  }

  function handleRowLimitChange(): void {
    dispatch('rowLimitChange', rowLimitValue);
  }

  function valueAsString(value: unknown): string {
    return typeof value === 'string' ? value : String(value);
  }
</script>

<section class="panel">
  <section class="detail">
    <div class="detail-head">
      <h2 id="tableTitle">{tableTitle}</h2>

      <label for="tableSelect">Table</label>
      <select id="tableSelect" bind:value={selectedTableValue} on:change={handleTableChange}>
        {#each tableOptions as option}
          <option value={option.value}>{option.label}</option>
        {/each}
      </select>

      <label for="rowLimit">Rows</label>
      <select id="rowLimit" bind:value={rowLimitValue} on:change={handleRowLimitChange}>
        {#each rowLimitOptions as option}
          <option value={option}>{option}</option>
        {/each}
      </select>
    </div>

    <div id="schemaCard" class="card" class:muted={schemaMuted}>{schemaText}</div>

    <div class="table-wrap">
      <table id="previewTable">
        <thead id="previewHead">
          {#if columns.length === 0}
            <tr>
              <th>No schema available</th>
            </tr>
          {:else}
            <tr>
              {#each columns as column}
                <th>{column}</th>
              {/each}
            </tr>
          {/if}
        </thead>

        <tbody id="previewBody">
          {#if columns.length > 0 && rows.length === 0}
            <tr>
              <td colspan={columns.length}>No rows found for this table.</td>
            </tr>
          {:else}
            {#each rows as row}
              <tr>
                {#each columns as column}
                  {@const value = row[column]}
                  <td>
                    {#if value === null || value === undefined}
                      {''}
                    {:else if selectedTable === 'web_searches' && column === 'result_url' && isHttpUrl(value)}
                      <a href={value} target="_blank" rel="noopener noreferrer">{value}</a>
                    {:else if selectedTable === 'web_searches' && column === 'search_query' && valueAsString(value).trim().length > 0}
                      <a href={searchQueryUrl(valueAsString(value))} target="_blank" rel="noopener noreferrer">
                        {valueAsString(value)}
                      </a>
                    {:else}
                      {stringifyCellValue(value)}
                    {/if}
                  </td>
                {/each}
              </tr>
            {/each}
          {/if}
        </tbody>
      </table>
    </div>
  </section>
</section>
