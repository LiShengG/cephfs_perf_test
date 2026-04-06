import { useState } from "react";

const palette = ["#ff9830", "#33a2e5", "#73bf69", "#f2495c", "#b877f3", "#ffd85e"];

function formatTimestamp(value) {
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return "-";
  }
  return date.toLocaleString();
}

export default function LineChart({ data, xAxisLabel = "Time", yAxisLabel = "Value" }) {
  const [hoverPoint, setHoverPoint] = useState(null);
  const entries = Object.entries(data?.series || {});
  if (!entries.length) {
    return <div className="chart-empty">No data for this metric.</div>;
  }

  const width = 760;
  const height = 260;
  const padLeft = 60;
  const padRight = 18;
  const padTop = 24;
  const padBottom = 44;
  const points = entries.flatMap(([, series]) => series);
  const numericValues = points.map((point) => Number(point.value)).filter(Number.isFinite);
  const numericTimes = points.map((point) => Date.parse(point.ts || "")).filter(Number.isFinite);

  if (!numericValues.length || !numericTimes.length) {
    return <div className="chart-empty">Metric exists but no numeric samples are available.</div>;
  }

  const minValue = Math.min(...numericValues);
  const maxValue = Math.max(...numericValues);
  const minTime = Math.min(...numericTimes);
  const maxTime = Math.max(...numericTimes);
  const valueSpan = maxValue - minValue || 1;
  const timeSpan = maxTime - minTime || 1;
  const plotWidth = width - padLeft - padRight;
  const plotHeight = height - padTop - padBottom;

  const seriesData = entries.map(([name, series], index) => {
    const stroke = palette[index % palette.length];
    const chartPoints = series
      .map((item) => {
        const ts = Date.parse(item.ts || "");
        const value = Number(item.value);
        if (!Number.isFinite(ts) || !Number.isFinite(value)) {
          return null;
        }
        const x = padLeft + ((ts - minTime) / timeSpan) * plotWidth;
        const y = padTop + plotHeight - ((value - minValue) / valueSpan) * plotHeight;
        return {
          x,
          y,
          ts,
          value,
          label: name,
        };
      })
      .filter(Boolean);

    return {
      name,
      stroke,
      points: chartPoints,
      polyline: chartPoints.map((point) => `${point.x.toFixed(2)},${point.y.toFixed(2)}`).join(" "),
    };
  });

  function handlePointerMove(event) {
    const svg = event.currentTarget;
    const bounds = svg.getBoundingClientRect();
    const mouseX = ((event.clientX - bounds.left) / bounds.width) * width;
    const mouseY = ((event.clientY - bounds.top) / bounds.height) * height;

    let nearest = null;
    let nearestDistance = Number.POSITIVE_INFINITY;
    for (const series of seriesData) {
      for (const point of series.points) {
        const distance = Math.abs(point.x - mouseX) + Math.abs(point.y - mouseY) * 0.35;
        if (distance < nearestDistance) {
          nearestDistance = distance;
          nearest = { ...point, stroke: series.stroke };
        }
      }
    }

    setHoverPoint(nearest);
  }

  return (
    <div className="chart-wrap">
      <svg
        viewBox={`0 0 ${width} ${height}`}
        className="chart-svg"
        preserveAspectRatio="none"
        onMouseMove={handlePointerMove}
        onMouseLeave={() => setHoverPoint(null)}
      >
        <rect x="0" y="0" width={width} height={height} fill="#111827" />
        <line x1={padLeft} y1={height - padBottom} x2={width - padRight} y2={height - padBottom} stroke="#2f3b52" />
        <line x1={padLeft} y1={padTop} x2={padLeft} y2={height - padBottom} stroke="#2f3b52" />
        {seriesData.map((series) => (
          <polyline key={series.name} fill="none" stroke={series.stroke} strokeWidth="2.8" points={series.polyline} />
        ))}
        {hoverPoint ? (
          <>
            <line
              x1={hoverPoint.x}
              y1={padTop}
              x2={hoverPoint.x}
              y2={height - padBottom}
              stroke={hoverPoint.stroke}
              strokeDasharray="4 4"
              opacity="0.65"
            />
            <line
              x1={padLeft}
              y1={hoverPoint.y}
              x2={width - padRight}
              y2={hoverPoint.y}
              stroke={hoverPoint.stroke}
              strokeDasharray="4 4"
              opacity="0.35"
            />
            <circle cx={hoverPoint.x} cy={hoverPoint.y} r="4.5" fill={hoverPoint.stroke} stroke="#111827" strokeWidth="2" />
          </>
        ) : null}
        <text x={padLeft} y="16" fill="#8fa2bf" fontSize="11">
          min={minValue.toFixed(2)} max={maxValue.toFixed(2)}
        </text>
        <text x={width / 2} y={height - 10} fill="#8fa2bf" fontSize="12" textAnchor="middle">
          {xAxisLabel}
        </text>
        <text
          x="16"
          y={height / 2}
          fill="#8fa2bf"
          fontSize="12"
          textAnchor="middle"
          transform={`rotate(-90 16 ${height / 2})`}
        >
          {yAxisLabel}
        </text>
        <text x={padLeft} y={height - padBottom + 18} fill="#8fa2bf" fontSize="11">
          {formatTimestamp(minTime)}
        </text>
        <text x={width - padRight} y={height - padBottom + 18} fill="#8fa2bf" fontSize="11" textAnchor="end">
          {formatTimestamp(maxTime)}
        </text>
        <text x={padLeft - 8} y={padTop + 4} fill="#8fa2bf" fontSize="11" textAnchor="end">
          {maxValue.toFixed(2)}
        </text>
        <text x={padLeft - 8} y={height - padBottom + 4} fill="#8fa2bf" fontSize="11" textAnchor="end">
          {minValue.toFixed(2)}
        </text>
      </svg>
      <div className="chart-coordinates">
        {hoverPoint ? (
          <>
            <span>{hoverPoint.label}</span>
            <span>X: {formatTimestamp(hoverPoint.ts)}</span>
            <span>Y: {hoverPoint.value.toFixed(4)}</span>
          </>
        ) : (
          <>
            <span>Move cursor over the chart</span>
            <span>X: {xAxisLabel}</span>
            <span>Y: {yAxisLabel}</span>
          </>
        )}
      </div>
      <div className="legend-row">
        {entries.map(([name], index) => (
          <span className="legend-chip" key={name}>
            <span className="legend-dot" style={{ backgroundColor: palette[index % palette.length] }} />
            {name}
          </span>
        ))}
      </div>
    </div>
  );
}
