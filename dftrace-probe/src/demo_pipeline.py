from pathlib import Path

from src.dftrace import TraceSession

def make_signal(n: int) -> list[float]:
    return [((i * 17) % 101) / 100.0 for i in range(n)]


def normalize(values: list[float]) -> list[float]:
    maximum = max(abs(x) for x in values) or 1.0
    return [x / maximum for x in values]


def mean(values: list[float]) -> float:
    return sum(values) / len(values)


def energy(values: list[float]) -> float:
    return sum(x * x for x in values)


def make_report(stats: tuple[float, float]) -> dict[str, float]:
    average, total_energy = stats
    return {"mean": average, "energy": total_energy}


def main() -> None:
    """ 
    source → normalize ─┬→ mean ───┐
                        └→ energy ─┴→ report
    """

    signal_size = 100000 

    with TraceSession(workers =2) as trace: 
        print(f"размер сигнала {signal_size}")
        print() 
        print ("создание сигнала")
        source_id, signal = trace.run("source", make_signal, signal_size)

        print(f"   source_id: {source_id}")
        print(f"   размер сигнала: {len(signal)} элементов")
        print()

        print("нормализация")
        normalize_id, normalized = trace.run("normalize", normalize, signal, source_id)
        print(f"   normalize_id: {normalize_id}")
        print()

        print("Параллельное вычисление mean и energy...")
        branches = trace.run_paralell(
            [("mean", mean), ("energy", energy)],
            normalized,
            normalize_id,
        )
    
        mean_id, mean_value = branches[0]
        energy_id, energy_value = branches[1]
        print(f"   mean_id: {mean_id}, значение: {mean_value:.6f}")
        print(f"   energy_id: {energy_id}, значение: {energy_value:.6f}")
        print()

        report_id, report = trace.run_join(
            "report",
            make_report,
            branches,  
        )
        print(f"   report_id: {report_id}")
        print(f"   отчёт: {report}")
        print()
        

        print("Сохраняем трассировку...")
        trace.save_json("results/trace.json")
        Path("results/trace.dot").write_text(trace.to_dot())
        
        
        print()
        print("Статистика трассировки:")
        print(f"   Узлов: {len(trace.nodes)}")
        print(f"   Рёбер: {len(trace.edges)}")
        print()
        
        
        expected_nodes = 5  # source, normalize, mean, energy, report
        expected_edges = 5  # source→normalize, normalize→mean, normalize→energy, mean→report, energy→report
        
        if len(trace.nodes) == expected_nodes:
            print(f"Количество узлов: {len(trace.nodes)} (ожидалось {expected_nodes})")
        else:
            print(f"Количество узлов: {len(trace.nodes)} (ожидалось {expected_nodes})")
        
        if len(trace.edges) == expected_edges:
            print(f"Количество рёбер: {len(trace.edges)} (ожидалось {expected_edges})")
        else:
            print(f" Количество рёбер: {len(trace.edges)} (ожидалось {expected_edges})")
        
        print()
        print("Pipeline успешно выполнен!")
        print(f"   JSON: results/trace.json")
        print(f"   DOT:  results/trace.dot")

if __name__ == "__main__": 
    main()