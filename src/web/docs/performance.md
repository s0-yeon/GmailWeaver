---
layout: default
title: Performance Guide
nav_order: 5
description: "Performance optimization strategies for Gentelella Admin Template"
---

# Performance Guide
{: .no_toc }

Optimization strategies, bundle analysis, and best practices for fast page loads.
{: .fs-6 .fw-300 }

## Table of contents
{: .no_toc .text-delta }

1. TOC
{:toc}

---

## Performance Overview

Gentelella v2.0 achieves significant performance gains through Vite's build system, code splitting, and dynamic module loading.

| Metric | Before (v1) | After (v2) | Improvement |
| ------ | ----------- | ---------- | ----------- |
| Initial Bundle Size | 779 KB | 79 KB | **90% smaller** |
| Total Page Load | 1.3 MB | 770 KB | **40% reduction** |
| First Contentful Paint | 2.1s | 0.8s | **62% faster** |
| Time to Interactive | 3.5s | 1.2s | **66% faster** |

---

## Code Splitting Strategy

### Dynamic Module Loading

Pages load only the modules they need via `window.loadModule()`:

```javascript
// Only loads Chart.js when chart containers exist
if (document.querySelector('.chart-container')) {
  const charts = await loadModule('charts');
}
```

### Manual Chunks

Vite's `manualChunks` configuration splits vendor libraries into optimally-sized bundles:

| Chunk | Size (gzip) | Contents | Used By |
| ----- | ----------- | -------- | ------- |
| `vendor-core` | 23 KB | Bootstrap, Popper.js | All pages |
| `vendor-chartjs` | 68 KB | Chart.js | Chart pages |
| `vendor-echarts` | 359 KB | ECharts | echarts.html only |
| `vendor-calendar` | 74 KB | FullCalendar | calendar.html |
| `vendor-maps` | 42 KB | Leaflet | map.html |
| `vendor-forms` | 49 KB | Choices.js, nouislider, Tempus Dominus | Form pages |
| `vendor-tables` | 63 KB | DataTables core | Table pages |
| `vendor-tables-ext` | 49 KB | DataTables extensions + JSZip | tables_dynamic.html |
| `vendor-ui` | 2 KB | NProgress | All pages |
| `vendor-utils` | 6 KB | Day.js, Skycons | Dashboard pages |

---

## Bundle Analysis

Run the built-in bundle analyzer to visualize chunk sizes:

```bash
npm run analyze
```

This generates an interactive treemap at `dist/stats.html` showing all chunks, their sizes, and dependencies.

### What to Look For

- **Unexpectedly large chunks** that may need further splitting
- **Duplicate dependencies** appearing in multiple chunks
- **Unused exports** that tree-shaking should eliminate

---

## Optimization Techniques

### 1. Conditional Module Loading

Guard all module imports with DOM checks:

```javascript
// Good - only loads DataTables when tables exist
if (document.querySelector('[data-datatable]')) {
  const tables = await loadModule('tables');
}

// Bad - loads DataTables on every page
const tables = await loadModule('tables');
```

### 2. CSS Optimization

- Source maps are disabled in production builds (saves ~8 MB)
- CSS is extracted and minified automatically by Vite
- Use SCSS variables and custom properties to reduce duplication

### 3. JavaScript Minification

Production builds use Terser with aggressive settings:

- `drop_console: true` - Removes all console statements
- `drop_debugger: true` - Removes debugger statements
- `passes: 3` - Multiple compression passes
- `dead_code: true` - Eliminates unreachable code

### 4. Build Target

The build targets `es2022`, enabling modern syntax that produces smaller output without polyfills.

---

## Browser Support

Modern browsers only (no IE11 polyfills needed):

| Browser | Minimum Version |
| ------- | --------------- |
| Chrome | 88+ |
| Firefox | 85+ |
| Safari | 14+ |
| Edge | 88+ |

---

## Monitoring Performance

### Development

```bash
# Start dev server with debug logging
npm run dev:debug

# Build and check chunk sizes
npm run build

# Full bundle analysis
npm run analyze
```

### Production Checks

1. Run `npm run build` and review the chunk size summary
2. Check `dist/stats.html` for the interactive treemap
3. Monitor the `vendor-echarts` chunk (largest at ~359 KB gzip) - only loaded on the ECharts page
4. Ensure new dependencies are assigned to appropriate manual chunks in `vite.config.js`
