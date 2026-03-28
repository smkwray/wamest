import { Routes, Route } from "react-router-dom";
import Layout from "./components/Layout";
import Home from "./pages/Home";
import Methods from "./pages/Methods";
import Limitations from "./pages/Limitations";

export default function App() {
  return (
    <Routes>
      <Route element={<Layout />}>
        <Route index element={<Home />} />
        <Route path="methods" element={<Methods />} />
        <Route path="limitations" element={<Limitations />} />
      </Route>
    </Routes>
  );
}
