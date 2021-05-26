import survey_personal_incomes
from survey_personal_incomes.save import save
from argparse import ArgumentParser


def main():
    parser = ArgumentParser(
        description="Utility to manage Survey of Personal Incomes microdata."
    )
    parser.add_argument(
        "action", choices=["save"], type=str, help="Save a UKDA SPI download."
    )
    parser.add_argument(
        "--path", type=str, help="The path to the microdata download."
    )
    parser.add_argument(
        "--year",
        choices=range(2010, 2020),
        type=int,
        help="The year of the SPI.",
    )
    parser.add_argument(
        "--zipped",
        action="store_true",
        help="Whether the download is zipped or a folder.",
    )
    args = parser.parse_args()

    if args.action == "save":
        if args.path is None or args.year is None:
            print("A path and year must be provided.")
            exit(1)
        save(folder=args.path, year=args.year, zipped=args.zipped)
