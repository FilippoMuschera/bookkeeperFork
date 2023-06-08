classes_dir=$1

echo java -jar ba-dua-cli-0.6.0-all.jar report -classes "$classes_dir" --sc -input "$classes_dir/coverage.ser" -xml "$classes_dir/target/baduaReport.xml"

java -jar "ba-dua-cli-0.6.0-all.jar report" -classes "$classes_dir" --sc -input "$classes_dir/coverage.ser" -xml "$classes_dir/target/baduaReport.xml"
