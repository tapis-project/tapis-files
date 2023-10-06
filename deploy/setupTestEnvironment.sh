
docker exec -i ssh-1 /bin/bash << EOF
mkdir -p /data/home/testuser/
chown -R testuser:testuser /data/home/testuser/
apt update
apt-get install acl
groupadd faclgrp1
groupadd faclgrp2
groupadd faclgrp3
useradd -g faclgrp1 facluser1
useradd -g faclgrp1 facluser2
useradd -g faclgrp1 facluser3
EOF

docker exec -i ssh-2 /bin/bash << EOF
mkdir -p /data/home/testuser/
chown -R testuser:testuser /data/home/testuser/
apt update
apt-get install acl
groupadd faclgrp1
groupadd faclgrp2
groupadd faclgrp3
useradd -g faclgrp1 facluser1
useradd -g faclgrp1 facluser2
useradd -g faclgrp1 facluser3
EOF
