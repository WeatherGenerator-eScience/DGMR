"""Generates a CSV file with labels indicating whether each radar image contains rain.

Note: Charlotte published two scripts: rainy day labeler and heavy rain labeler.
They use different thresholds for determining if a radar image contains rain and for identfictaion of clutter.
TODO: verify which thresholds to use.

As opposed to Charlotte's scripts, this code generates a single CSV file with labels for all radar images.
"""

import logging
from pathlib import Path
import pandas as pd
from skimage import morphology

import numpy as np
from tqdm import tqdm
import h5py


logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

CLUTTERMASK = np.load(Path(__file__).parent / "cluttermask.npy")


def read_radar_file(file: Path) -> np.ndarray:
    with h5py.File(file, "r") as f:
        image = f["image1"]["image_data"][:]

        ## Set pixels out of image to 0
        ooi_value = f["image1"]["calibration"].attrs["calibration_out_of_image"]
        image[image == ooi_value] = 0
        # Sometimes 255 or other number (244) is used for the calibration
        # for out of image values, so also check the first pixel
        image[image == image[0][0]] = 0

        return image


def has_clutter(image: np.ndarray, threshold=500, min_pixels=130) -> bool:
    """Determine if the radar image contains clutter.

    An image is considered cluttered if the gradient magnitude exceeds
    the threshold for at least `pixel_threshold` pixels.
    """
    gx, gy = np.gradient(image)
    magnitude_squared = gx**2 + gy**2
    clutter_pixels = magnitude_squared > threshold**2

    return np.sum(clutter_pixels) > min_pixels


def is_rainy(image: np.ndarray) -> bool:
    """Determine if the radar image contains rain.

    TODO: I don't understand where 30mm / 3000 comes from
    Compare to average computation in Charlotte's heavy rain labeler.
    """
    # Mask out blobs < 15 pixels and pixels that tend to contain clutter
    valid_objects = morphology.remove_small_objects(
        image > 0, min_size=9, connectivity=8
    )
    true_showers = image * valid_objects * CLUTTERMASK

    return np.sum(true_showers) > 3000 and not has_clutter(image)


def main():
    data_dir = Path("~/weathergenerator/data").expanduser()
    radar_files = sorted(data_dir.glob("*.h5"))

    results = []
    for f in tqdm(radar_files, desc="Processing radar files"):
        try:
            image = read_radar_file(f)
            rainy = is_rainy(image)
        except Exception as e:
            logger.error(f"Error processing {f}: {e}")
            rainy = None

        results.append({"filename": f.name, "rainy": rainy})

    df = pd.DataFrame(results)
    df.to_csv("rainy_labels.csv", index=False)


if __name__ == "__main__":
    main()
