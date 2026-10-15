from institutional_migrations_v2 import ensure_schema


def main():
    state = ensure_schema()
    print(f"institutional schema ready: {state}")


if __name__ == "__main__":
    main()
