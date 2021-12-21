import os
import subprocess
import getpass

username = getpass.getuser()

def gen_poolset(paths, num_shards, num_levels=3):
    poolset_types = ['log', 'value']
    for p in paths:
        if not os.path.isdir(p):
            subprocess.call(['sudo', 'mkdir', p])
            subprocess.call(['sudo', 'chown', '-R', '{}:{}'.format(username, username), p])
        for t in poolset_types:
            for s in range(0, num_shards):
                os.makedirs('{}/{}_{}'.format(p, t, s), exist_ok=True)
                f = open('{}/{}_{}.set'.format(p, t, s), 'w')
                lines = ['PMEMPOOLSET\n',
                         'OPTION SINGLEHDR\n',
                         '400G {}/{}_{}/\n'.format(p, t, s)]
                f.writelines(lines)
                f.close()
        # index pool per level
        for l in range(0, num_levels):
            for s in range(0, num_shards):
                os.makedirs('{}/level_{}/index_{}'.format(p, l, s), exist_ok=True)
                f = open('{}/level_{}/index_{}.set'.format(p, l, s), 'w')
                lines = ['PMEMPOOLSET\n',
                         'OPTION SINGLEHDR\n',
                         '400G {}/level_{}/index_{}/\n'.format(p, l, s)]
                f.writelines(lines)
                f.close()

def main():
    # Set the number of regions
    num_regions = int(input('Type the number of PMEM regions: '))
    # Set the primary PMEM region
    primary = int(input('Type the primary region (Default=0): ') or '0')
    # Set DB path
    path_prefix = input('Type the PMEM path format (use python String.Format style, type "custom" to configure manually. Default=/pmem{}/brdb_{}): '.format('{}', username)) or '/pmem{}/brdb_{}'.format('{}', username)
    node_paths = []
    if path_prefix == 'custom':
        for i in range(0, num_regions):
            p = input('Type the PMEM path for node {}: '.format(i))
            node_paths.append(p)
    else:
        for i in range(0, num_regions):
            p = path_prefix.format(i)
            node_paths.append(p)
    for p in node_paths:
        print(p)
    do_gen = input('OK? [Y/n]: ') or 'y'
    if do_gen.lower() != 'y':
        raise
    # Generate poolset files
    gen_poolset(node_paths, 1)
    # Save the configuration
    lines = ['NUM_REGIONS {}\n'.format(num_regions),
             'PRIMARY_REGION {}\n'.format(primary),
             'NUM_SHARDS {}\n'.format(1)]
    for p in node_paths:
        lines.append('{}\n'.format(p))
    os.makedirs('/tmp/brdb_{}/'.format(username), exist_ok=True)
    with open('/tmp/brdb_{}/config'.format(username), 'w') as f:
        f.writelines(lines)

if __name__ == '__main__':
    main()
