from gooey import Gooey
from gooey import GooeyParser


@Gooey(
    program_name="ibsg",
    timing_options={"show_time_remaining": True},
)
def ber_private():
    description = "Generate a csv file of individual Irish buildings"
    parser = GooeyParser(description=description)
    parser.add_argument(
        dest="InputFile",
        widget="FileChooser",
        help="Upload BER Small Area Extract CSV File",
    )
    parser.add_argument(
        dest="OutputDirectory",
        widget="DirChooser",
        help="Save Directory for Processed Data",
    )
    return parser.parse_args()


if __name__ == "__main__":
    ber_private()
