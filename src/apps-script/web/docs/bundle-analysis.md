---
layout: default
title: Bundle Analysis
nav_order: 9
---

# Bundle Analysis Guide

This guide explains how to use the bundle analyzer to monitor and optimize the bundle size of the Gentelella admin template.

## Quick Start

```bash
# Build and generate bundle analysis
npm run analyze

# Build without opening the stats file (for CI)
npm run analyze:ci
```

## Analysis File Location

After running the build, the bundle analysis is saved to:
- `dist/stats.html` - Interactive treemap visualization

## Understanding the Analysis

### Treemap View
The default treemap view shows:
- **Size of boxes** = Bundle size (larger boxes = larger bundles)
- **Colors** = Different modules and dependencies
- **Nested structure** = Module hierarchy and dependencies

### Key Metrics to Monitor

1. **Vendor Chunks** (largest bundles):
   - `vendor-echarts` (~359KB gzip) - ECharts (echarts.html only)
   - `vendor-calendar` (~74KB gzip) - FullCalendar
   - `vendor-chartjs` (~68KB gzip) - Chart.js
   - `vendor-tables` (~63KB gzip) - DataTables core
   - `vendor-tables-ext` (~49KB gzip) - DataTables extensions + JSZip
   - `vendor-forms` (~49KB gzip) - Choices.js, nouislider, Tempus Dominus
   - `vendor-maps` (~42KB gzip) - Leaflet
   - `vendor-core` (~23KB gzip) - Bootstrap, Popper.js

2. **Application Code**:
   - `main-core` - Core entry point (jQuery-free)
   - Page-specific bundles (dynamically loaded)

3. **CSS Bundles**:
   - Main stylesheet bundle
   - Page-specific CSS

## Optimization Strategies

### 1. Identify Large Dependencies
- Look for unexpectedly large vendor chunks
- Check if dependencies are being tree-shaken properly
- Consider lighter alternatives for heavy libraries

### 2. Monitor Bundle Growth
- Track changes in bundle sizes over time
- Set up alerts for significant size increases
- Use gzip/brotli compressed sizes for realistic network transfer sizes

### 3. Code Splitting Optimization
Current manual chunks are optimized for:

- **vendor-core**: Bootstrap + Popper.js loaded on every page
- **vendor-chartjs**: Chart.js (loaded only on chart pages)
- **vendor-echarts**: ECharts (loaded only on echarts.html)
- **vendor-forms**: Choices.js, nouislider, Tempus Dominus (form pages)
- **vendor-tables**: DataTables core (table pages)
- **vendor-maps**: Leaflet (map page only)
- **vendor-calendar**: FullCalendar (calendar page only)

### 4. Dynamic Import Opportunities
Consider converting large features to dynamic imports:
```javascript
// Instead of static import
import { Chart } from 'chart.js';

// Use dynamic import for conditional loading
if (document.querySelector('.chart-container')) {
  const { Chart } = await import('chart.js');
}
```

## Performance Targets

### Current Performance (as of latest build):

- **Initial Bundle**: 79KB (core + styles)
- **Total Page Load**: ~770KB (dashboard with all widgets)
- **Page Load Impact**: Core bundle (~23KB gzip) loads on every page

### Recommended Targets:

- **Core Bundle**: <50KB gzip (currently ~23KB ✅)
- **Feature Bundles**: <100KB gzip each (echarts: ~359KB ⚠️ - isolated to single page)
- **Total Initial Load**: <100KB gzipped (currently ~79KB ✅)

## Bundle Size Warnings

The build process will warn about chunks larger than 1000KB:

- This is currently triggered by the `vendor-echarts` bundle (~1,109KB uncompressed)
- Consider splitting chart libraries further or using dynamic imports
- Adjust the warning limit in `vite.config.js` if needed