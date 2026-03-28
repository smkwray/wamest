declare module "plotly.js-dist-min" {
  import Plotly from "plotly.js";
  export default Plotly;
}

declare module "react-plotly.js/factory" {
  import type { Component, CSSProperties } from "react";
  interface PlotParams {
    data: any[];
    layout?: any;
    config?: any;
    style?: CSSProperties;
    useResizeHandler?: boolean;
  }
  type PlotComponent = new () => Component<PlotParams>;
  export default function createPlotlyComponent(plotly: any): PlotComponent;
}
