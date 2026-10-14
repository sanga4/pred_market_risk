from institutional_migrations import ensure_schema


def main():
    state = ensure_schema()
    print(f"institutional schema ready: {state}")


if __name__ == "__main__":
    main()
