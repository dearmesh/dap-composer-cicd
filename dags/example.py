#testing 1, 2, 3

# 1
# 2
# 3 test not using TARGET_SUBS
# 4 Removed TARGET_BUCKET Substitution:
#   The bucket selection logic is now fully handled in the script using if conditions based on $BRANCH_NAME.
#   Dynamic Bucket Resolution:
#   The TARGET_BUCKET variable is assigned dynamically in the script, removing the need for any substitution.