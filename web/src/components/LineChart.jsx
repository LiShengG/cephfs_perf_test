const palette = ["#c95f36", "#1f7a8c", "#6c8c3c", "#8646b5", "#ba3f3a", "#cc8b1f"];

export default function LineChart({ data }) {
  const entries = Object.entries(data?.series || {});
  if (!entries.length) {
    return <div className="chart-empty">No data for this metric.</div>;
  }

  const width = 760;
  const height = 260;
  const pad = 28;
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

  return (
    <div className="chart-wrap">
      <svg viewBox={`0 0 ${width} ${height}`} className="chart-svg" preserveAspectRatio="none">
        <rect x="0" y="0" width={width} height={height} fill="#fffdfa" />
        <line x1={pad} y1={height - pad} x2={width - pad} y2={height - pad} stroke="#dbcdb6" />
        <line x1={pad} y1={pad} x2={pad} y2={height - pad} stroke="#dbcdb6" />
        {entries.map(([name, series], index) => {
          const stroke = palette[index % palette.length];
          const polyline = series
            .map((item) => {
              const ts = Date.parse(item.ts || "");
              const value = Number(item.value);
              if (!Number.isFinite(ts) || !Number.isFinite(value)) {
                return null;
              }
              const x = pad + ((ts - minTime) / timeSpan) * (width - pad * 2);
              const y = height - pad - ((value - minValue) / valueSpan) * (height - pad * 2);
              return `${x.toFixed(2)},${y.toFixed(2)}`;
            })
            .filter(Boolean)
            .join(" ");

          return <polyline key={name} fill="none" stroke={stroke} strokeWidth="2.8" points={polyline} />;
        })}
        <text x={pad} y="18" fill="#7a6c5b" fontSize="11">
          min={minValue.toFixed(2)} max={maxValue.toFixed(2)}
        </text>
      </svg>
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
