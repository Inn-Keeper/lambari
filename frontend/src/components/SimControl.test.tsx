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

  it("posts the rate once on release, not on every drag step", async () => {
    const f = vi.fn(async () => new Response("{}", { status: 200 }));
    vi.stubGlobal("fetch", f);
    render(<SimControl sim={{ running: true, rate: 5000 }} />);

    const thumb = screen.getByRole("slider");
    thumb.focus();
    // keyboard steps commit one by one in Radix; three steps, three commits max
    await userEvent.keyboard("{ArrowRight}{ArrowRight}{ArrowRight}");
    expect(screen.getByText("6,500 tx/s")).toBeInTheDocument();
    const bodies = f.mock.calls.map((c) => JSON.parse((c as unknown as [string, RequestInit])[1].body as string));
    expect(bodies.at(-1)).toEqual({ rate: 6500 });
  });

  it("keeps the rate within the server's cap", async () => {
    const f = vi.fn(async () => new Response("{}", { status: 200 }));
    vi.stubGlobal("fetch", f);
    render(<SimControl sim={{ running: false, rate: 0, max_rate: 1000 }} />);

    expect(screen.getByText("1,000 tx/s")).toBeInTheDocument(); // 5000 default, clamped
    await userEvent.click(getSwitch());
    const [, init] = f.mock.calls[0] as unknown as [string, RequestInit];
    expect(JSON.parse(init.body as string)).toEqual({ rate: 1000 });
  });
});
