from subprocess import run


def step(cmd: str):
    print(f"\n▶ {cmd}")
    run(cmd, shell=True, check=True)


def main():
    print("🚀 START BUSINESS MONITORING PIPELINE")

    step("python src/generate_data.py")
    step("python src/validate.py")
    step("python src/clean.py")
    step("python src/validate.py data/processed/orders_clean.csv")
    step("python src/alerts.py")
    step("python src/summary.py")

    print("\n✅ PIPELINE FINISHED SUCCESSFULLY")
    print("📁 Revisa la carpeta /reports")


if __name__ == "__main__":
    main()