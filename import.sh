#!/usr/bin/env bash
#sudo apt-get install python-pip python-dev build-essential
#$ sudo pip install --upgrade pip
#$ sudo pip install --upgrade virtualenv
# sudo pip install rethinkdb
rethinkdb import -f data/AD_meta.xml.json --table test.users --force
rethinkdb import -f data/AL_meta.xml.json --table test.users --force
rethinkdb import -f data/BA_meta.xml.json --table test.users --force
rethinkdb import -f data/BE_meta.xml.json --table test.users --force
rethinkdb import -f data/BG_meta.xml.json --table test.users --force
rethinkdb import -f data/CH_meta.xml.json --table test.users --force
rethinkdb import -f data/CY_meta.xml.json --table test.users --force
rethinkdb import -f data/CZ_meta.xml.json --table test.users --force
rethinkdb import -f data/DK_meta.xml.json --table test.users --force
rethinkdb import -f data/EE_meta.xml.json --table test.users --force
rethinkdb import -f data/FI_meta.xml.json --table test.users --force
rethinkdb import -f data/GR_meta.xml.json --table test.users --force
rethinkdb import -f data/HR_meta.xml.json --table test.users --force
rethinkdb import -f data/HU_meta.xml.json --table test.users --force
rethinkdb import -f data/IE_meta.xml.json --table test.users --force
rethinkdb import -f data/IS_meta.xml.json --table test.users --force
rethinkdb import -f data/LI_meta.xml.json --table test.users --force
rethinkdb import -f data/LT_meta.xml.json --table test.users --force
rethinkdb import -f data/LU_meta.xml.json --table test.users --force
rethinkdb import -f data/LV_meta.xml.json --table test.users --force
rethinkdb import -f data/ME_meta.xml.json --table test.users --force
rethinkdb import -f data/MK_meta.xml.json --table test.users --force
rethinkdb import -f data/MT_meta.xml.json --table test.users --force
rethinkdb import -f data/NL_meta.xml.json --table test.users --force
rethinkdb import -f data/NO_meta.xml.json --table test.users --force
rethinkdb import -f data/PL_meta.xml.json --table test.users --force
rethinkdb import -f data/PT_meta.xml.json --table test.users --force
rethinkdb import -f data/RO_meta.xml.json --table test.users --force
rethinkdb import -f data/RS_meta.xml.json --table test.users --force
rethinkdb import -f data/SE_meta.xml.json --table test.users --force
rethinkdb import -f data/SI_meta.xml.json --table test.users --force
rethinkdb import -f data/SK_meta.xml.json --table test.users --force
rethinkdb import -f data/TR_meta.xml.json --table test.users --force
rethinkdb import -f data/XK_meta.xml.json --table test.users --force

rethinkdb import -f data/GB_meta.xml.json --table test.users --force --change-read-chunk-size=5120000 --change-max-buffer-size=934217728
rethinkdb import -f data/AT_meta.xml.json --table test.users --force --change-read-chunk-size=5120000 --change-max-buffer-size=934217728
rethinkdb import -f data/FR_meta.xml.json --table test.users --force --change-read-chunk-size=5120000 --change-max-buffer-size=934217728
rethinkdb import -f data/IT_meta.xml.json --table test.users --force --change-read-chunk-size=5120000 --change-max-buffer-size=934217728
rethinkdb import -f data/ES_meta.xml.json --table test.users --force --change-read-chunk-size=5120000 --change-max-buffer-size=934217728
rethinkdb import -f data/DE_meta.xml.json --table test.users --force --change-read-chunk-size=5120000 --change-max-buffer-size=934217728



