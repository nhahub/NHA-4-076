from merge_engine import build_dataset
from pass_detector import detect_passes


# Build dataset directly (no file dependency)
df = build_dataset()

passes = detect_passes(df)

print("Number of passes:", len(passes))

for i, p in enumerate(passes, 1):
    print(f"\nPASS {i}")
    print(f"AOS: {p.aos}")
    print(f"LOS: {p.los}")
    print(f"TCA: {p.tca}")
    print(f"Max Elevation: {p.max_elevation:.2f} deg")
    print(f"Duration: {p.duration:.1f} sec")