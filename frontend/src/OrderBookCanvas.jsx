import { useRef, useEffect } from 'react';
import ReactECharts from 'echarts-for-react';

/**
 * L2 order book depth heatmap rendered via ECharts + HTML5 Canvas.
 * Uses Canvas API (not SVG) to handle 100k-point tick-by-tick updates
 * without DOM bottleneck. See architecture.md §7 — Live Operations UI.
 */
export default function OrderBookCanvas({ bookData, signal }) {
  const getOption = () => {
    if (!bookData) return {};

    const { bids = [], asks = [] } = bookData;

    return {
      backgroundColor: '#0d1117',
      grid: { left: 60, right: 20, top: 20, bottom: 40 },
      xAxis: { type: 'value', name: 'Cumulative Volume', nameTextStyle: { color: '#8b949e' } },
      yAxis: { type: 'value', name: 'Price', nameTextStyle: { color: '#8b949e' } },
      series: [
        {
          name: 'Bids',
          type: 'bar',
          data: bids.map(([price, vol]) => [vol, price]),
          itemStyle: { color: 'rgba(56, 211, 159, 0.7)' },
        },
        {
          name: 'Asks',
          type: 'bar',
          data: asks.map(([price, vol]) => [vol, price]),
          itemStyle: { color: 'rgba(248, 81, 73, 0.7)' },
        },
        // AI signal marker
        ...(signal
          ? [{
              name: 'AI Signal',
              type: 'scatter',
              data: [[signal.volume, signal.price]],
              symbol: 'triangle',
              symbolSize: 14,
              itemStyle: { color: '#f0b429' },
            }]
          : []),
      ],
    };
  };

  return (
    <ReactECharts
      option={getOption()}
      style={{ height: '500px', width: '100%' }}
      opts={{ renderer: 'canvas' }}
    />
  );
}
