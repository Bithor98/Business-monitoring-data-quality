from pathlib import Path
from subprocess import run


def step(args):
    print(f"\n▶ Ejecutando: {' '.join(args)}")
    run(args, check=True)


def main():
    root = Path(__file__).parent
    py = str(root / ".venv" / "Scripts" / "python.exe")  # fuerza python del venv

    print("🚀 START BUSINESS MONITORING PIPELINE")

    step([py, "src/generate_data.py"])
    step([py, "src/validate.py"])
    step([py, "src/clean.py"])
    step([py, "src/validate.py", "data/processed/orders_clean.csv"])
    step([py, "src/alerts.py"])
    step([py, "src/summary.py"])

    print("\n✅ PIPELINE FINISHED SUCCESSFULLY")
    print("📁 Revisa la carpeta /reports")


if __name__ == "__main__":
    main()