
import { defineConfig, type DefaultTheme, type UserConfig } from "vitepress"
import llmstxt from 'vitepress-plugin-llms'

const config: UserConfig<DefaultTheme.Config> = {
  base: '/',
  lang: 'en-US',
  title: 'Polecat',
  description: '.NET Event Store and Document Database on SQL Server 2025',
  head: [
    ['meta', { property: 'og:title', content: 'Polecat' }],
    ['meta', { property: 'og:type', content: 'website' }],
    ['meta', { property: 'og:description', content: '.NET Event Store and Document Database on SQL Server 2025' }],
  ],

  lastUpdated: true,

  themeConfig: {
    logo: '/logo.png',

    nav: [
      { text: 'Intro', link: '/introduction' },
      { text: 'Document DB', link: '/documents/', activeMatch: '/documents/' },
      { text: 'Event Store', link: '/events/', activeMatch: '/events/' },
      { text: 'Support Plans', link: 'https://www.jasperfx.net/support-plans/' },
    ],

    search: {
      provider: 'local'
    },

    editLink: {
      pattern: 'https://github.com/JasperFx/polecat/edit/main/docs/:path',
      text: 'Suggest changes to this page'
    },

    socialLinks: [
      { icon: 'github', link: 'https://github.com/JasperFx/polecat' },
    ],

    footer: {
      message: 'Released under the MIT License.',
      copyright: 'Copyright \u00a9 Jeremy D. Miller and contributors.',
    },

    sidebar: {
      '/': [
        {
          text: 'Introduction',
          collapsed: true,
          items: [
            { text: 'What is Polecat?', link: '/introduction' },
            { text: 'Getting Started', link: '/getting-started' },
          ]
        },
        {
          text: 'Configuration',
          collapsed: true,
          items: [
            { text: 'Bootstrapping Polecat', link: '/configuration/hostbuilder' },
            { text: 'Configuring Document Storage', link: '/configuration/storeoptions' },
            { text: 'JSON Serialization', link: '/configuration/json' },
            { text: 'Resiliency Policies', link: '/configuration/retries' },
            { text: 'Multi-Tenancy with Database per Tenant', link: '/configuration/multitenancy' },
            { text: 'MCP Server', link: '/configuration/mcp' },
          ]
        },
        {
          text: 'Document Database',
          collapsed: true,
          items: [
            { text: 'Polecat as Document DB', link: '/documents/' },
            { text: 'Document Identity', link: '/documents/identity' },
            { text: 'Database Storage', link: '/documents/storage' },
            { text: 'Polecat Metadata', link: '/documents/metadata' },
            { text: 'Opening Sessions', link: '/documents/sessions' },
            { text: 'Storing Documents', link: '/documents/storing' },
            { text: 'Deleting Documents', link: '/documents/deletes' },
            {
              text: 'Querying Documents', link: '/documents/querying/', collapsed: true, items: [
                { text: 'Loading Documents by Id', link: '/documents/querying/byid' },
                { text: 'Querying Documents with LINQ', link: '/documents/querying/linq/' },
                { text: 'Supported LINQ Operators', link: '/documents/querying/linq/operators' },
                { text: 'Querying within Child Collections', link: '/documents/querying/linq/child-collections' },
                { text: 'Searching on String Fields', link: '/documents/querying/linq/strings' },
                { text: 'Paging', link: '/documents/querying/linq/paging' },
                { text: 'Querying for Raw JSON', link: '/documents/querying/query-json' },
                { text: 'Batched Queries', link: '/documents/querying/batched-queries' },
              ]
            },
            { text: 'Multi-Tenanted Documents', link: '/documents/multi-tenancy' },
            { text: 'Initial Baseline Data', link: '/documents/initial-data' },
            { text: 'Optimistic Concurrency', link: '/documents/concurrency' },
            { text: 'Partial Updates/Patching', link: '/documents/partial-updates-patching' },
            { text: 'Bulk Insert', link: '/documents/bulk-insert' },
          ]
        },
        {
          text: 'Event Store',
          collapsed: true,
          items: [
            { text: 'Polecat as Event Store', link: '/events/' },
            { text: 'Quick Start', link: '/events/quickstart' },
            { text: 'Storage', link: '/events/storage' },
            { text: 'Appending Events', link: '/events/appending' },
            { text: 'Querying Events', link: '/events/querying' },
            { text: 'Metadata', link: '/events/metadata' },
            { text: 'Archiving Streams', link: '/events/archiving' },
            { text: 'Snapshots', link: '/events/snapshots' },
            { text: 'Natural Keys', link: '/events/natural-keys' },
            { text: 'Dynamic Consistency Boundary', link: '/events/dcb' },
            {
              text: 'Projections Overview', link: '/events/projections/', collapsed: true, items: [
                { text: 'Single Stream Projections', link: '/events/projections/single-stream-projections' },
                { text: 'Multi Stream Projections', link: '/events/projections/multi-stream-projections' },
                { text: 'Event Projections', link: '/events/projections/event-projections' },
                { text: 'Live Aggregations', link: '/events/projections/live-aggregates' },
                { text: 'Inline Projections', link: '/events/projections/inline' },
                { text: 'Flat Table Projections', link: '/events/projections/flat' },
                { text: 'Composite Projections', link: '/events/projections/composite' },
                { text: 'Asynchronous Projections', link: '/events/projections/async-daemon' },
                { text: 'EF Core Projections', link: '/events/projections/efcore' },
              ]
            },
            {
              text: 'Event Subscriptions',
              link: '/events/subscriptions'
            },
            {
              text: 'Multi-Tenancy',
              link: '/events/multitenancy'
            },
          ]
        },
        {
          text: 'Testing',
          collapsed: true,
          items: [
            { text: 'Integration Testing', link: '/testing/integration' },
          ]
        },
        {
          text: 'Diagnostics',
          collapsed: true,
          items: [
            { text: 'Diagnostics and Instrumentation', link: '/diagnostics' },
          ]
        },
        {
          text: 'Schema',
          collapsed: true,
          items: [
            { text: 'Database Management', link: '/schema/' },
            { text: 'How Documents are Stored', link: '/schema/storage' },
            { text: 'Schema Migrations', link: '/schema/migrations' },
            { text: 'Exporting Schema Definition', link: '/schema/exporting' },
            { text: 'Tearing Down Document Storage', link: '/schema/cleaning' },
          ]
        },
      ]
    }
  },
  vite: {
    plugins: [llmstxt()],
    build: {
      chunkSizeWarningLimit: 3000
    }
  }
}

export default defineConfig(config)
