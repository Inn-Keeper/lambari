import { useEffect, useState } from "react";
import * as Slider from "@radix-ui/react-slider";
import * as Switch from "@radix-ui/react-switch";
import { setSimulation, type SimState } from "../lib/useStream";

export function SimControl({ sim }: { sim: SimState }) {
  const [rate, setRate] = useState(5000);
  const [running, setRunning] = useState(false);

  // keep local state in sync when the server reports sim status
  useEffect(() => {
    setRunning(sim.running);
    if (sim.running && sim.rate > 0) setRate(sim.rate);
  }, [sim.running, sim.rate]);

  const toggle = async (on: boolean) => {
    setRunning(on);
    await setSimulation(on ? rate : 0);
  };

  const changeRate = async (v: number) => {
    setRate(v);
    if (running) await setSimulation(v);
  };

  return (
    <div className="flex items-center gap-4">
      <div className="flex items-center gap-3">
        <span className="font-mono text-sm tabular-nums text-muted">
          {rate.toLocaleString()} tx/s
        </span>
        <Slider.Root
          className="relative flex h-5 w-36 touch-none select-none items-center"
          min={500}
          max={20000}
          step={500}
          value={[rate]}
          onValueChange={([v]) => changeRate(v)}
          aria-label="Simulated transactions per second"
        >
          <Slider.Track className="relative h-1 grow rounded-full bg-panel-2">
            <Slider.Range className="absolute h-full rounded-full bg-accent" />
          </Slider.Track>
          <Slider.Thumb className="block h-4 w-4 rounded-full bg-ink shadow focus:outline-none focus-visible:ring-2 focus-visible:ring-accent" />
        </Slider.Root>
      </div>

      <label className="flex items-center gap-2">
        <span className="text-sm text-muted">Simulator</span>
        <Switch.Root
          checked={running}
          onCheckedChange={toggle}
          className="relative h-6 w-11 rounded-full bg-panel-2 transition-colors data-[state=checked]:bg-approve focus-visible:ring-2 focus-visible:ring-accent focus:outline-none"
          aria-label="Start or stop the traffic simulator"
        >
          <Switch.Thumb className="block h-5 w-5 translate-x-0.5 rounded-full bg-ink transition-transform data-[state=checked]:translate-x-[22px]" />
        </Switch.Root>
      </label>
    </div>
  );
}
