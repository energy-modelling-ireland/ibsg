from gooey import Gooey
from gooey import GooeyParser


@Gooey(program_name="ibsg")
def gui():
    parser = GooeyParser(
        description="Generate a csv file of individual Irish buildings"
    )
    chosen_ber_dataset = parser.add_mutually_exclusive_group(
        required=True, gooey_options={"initial_selection": 0}
    )
    chosen_ber_dataset.add_argument(
        "--private-ber",
        dest="BER Public",
        action="store_true",
        help="Download the Open-Access BER Public Dataset",
    )
    chosen_ber_dataset.add_argument(
        "--public-ber",
        dest="BER Private",
        action="store_true",
        help="Upload the Closed-Access Small Area Extract of the BER Public Dataset",
        widget="FileChooser",
    )
    parser.parse_args()


if __name__ == "__main__":
    config = gui()
