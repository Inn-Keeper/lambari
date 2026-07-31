import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it, vi } from "vitest";
import { SimControl } from "./SimControl";

afterEach(() => vi.unstubAllGlobals());

// Radix renders the visible <button role="switch"> plus a hidden native
// input that jsdom also exposes as a switch — target the button.
const getSwitch = () =>
  screen.getAllByRole("switch").find((el) => el.tagName === "BUTTON")!;

describe("SimControl", () => {
  it("reverts the switch and shows an error when the POST fails", async () => {
    vi.stubGlobal("fetch", vi.fn(async () => new Response("boom", { status: 500 })));
    render(<SimControl sim={{ running: false, rate: 0 }} />);

    const sw = getSwitch();
    await userEvent.click(sw);

    await screen.findByRole("alert");
    expect(sw).toHaveAttribute("aria-checked", "false");
  });

  it("keeps the switch on when the POST succeeds", async () => {
    vi.stubGlobal("fetch", vi.fn(async () => new Response("{}", { status: 200 })));
    render(<SimControl sim={{ running: false, rate: 0 }} />);

    const sw = getSwitch();
    await userEvent.click(sw);
    expect(sw).toHaveAttribute("aria-checked", "true");
    expect(screen.queryByRole("alert")).not.toBeInTheDocument();
  });
});
