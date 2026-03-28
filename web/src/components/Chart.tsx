// @ts-nocheck
import factoryModule from "react-plotly.js/factory";
import Plotly from "plotly.js-dist-min";

// Vite CJS interop can double-wrap the default export
const createPlotlyComponent =
  typeof factoryModule === "function"
    ? factoryModule
    : factoryModule.default;

const Plot = createPlotlyComponent(Plotly);

export default function Chart(props: {
  data: any[];
  layout: any;
  config?: any;
  style?: React.CSSProperties;
}) {
  return (
    <Plot
      data={props.data}
      layout={props.layout}
      config={props.config ?? { responsive: true, displaylogo: false }}
      style={props.style ?? { width: "100%" }}
      useResizeHandler
    />
  );
}
