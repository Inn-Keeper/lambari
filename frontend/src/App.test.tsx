import { act, render, screen } from "@testing-library/react";
import { afterEach, expect, it, vi } from "vitest";
import App from "./App";

afterEach(() => {
  vi.useRealTimers();
  vi.unstubAllGlobals();
});

it("shows starting, then disconnected once the engine never answers", () => {
  vi.useFakeTimers();
  // An EventSource that never opens.
  vi.stubGlobal("EventSource", class { close() {} });

  render(<App />);
  expect(screen.getByRole("status")).toHaveTextContent("engine starting…");

  act(() => void vi.advanceTimersByTime(20_000));
  expect(screen.getByRole("status")).toHaveTextContent("disconnected");
});
