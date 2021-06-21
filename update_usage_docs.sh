#!/bin/bash

# This little bash script just updates the docs/usage.md file with
# latest usage, for the installed version.
# Run this when updating version to update the usage docs.
cp docs/usage.template usage.template

for cmd in "--help" "report --help" "inspect --help" "remove --help" "retrieve --help" "status --help" "status get --help" "status set --help"; do
	echo $cmd
	echo -e "## \`pipestat $cmd\`" > USAGE_header.temp
	pipestat $cmd --help > USAGE.temp 2>&1
	# sed -i 's/^/\t/' USAGE.temp
	sed -i.bak '1s;^;\`\`\`console\
;' USAGE.temp
#	sed -i '1s/^/\n\`\`\`console\n/' USAGE.temp
	echo -e "\`\`\`\n" >> USAGE.temp
	#sed -i -e "/\`looper $cmd\`/r USAGE.temp" -e '$G' usage.template  # for -in place inserts
	cat USAGE_header.temp USAGE.temp >> usage.template # to append to the end
done
rm USAGE.temp
rm USAGE_header.temp
rm USAGE.temp.bak
mv usage.template  docs/usage.md
cat docs/usage.md
#rm USAGE.temp
