RAW_PATH = "../text/hhs_grants_terminated_raw.txt"
PROJECT_NUMBER_PATH = "../text/hhs_grants_terminated_fains.txt"

def main():
    project_numbers = set()

    with open(RAW_PATH) as src:
        for line in src:
            line = line.split()
            if line[0] != "NIH":
                continue

            project_numbers.add(line[1])

    with open(PROJECT_NUMBER_PATH, "w") as dest:
        for entry in sorted(project_numbers):
            dest.write(entry + "\n")

if __name__ == "__main__":
    main()
