import getopt


# Get params from CMD
def get_arg(argv):
    """Get params from CMD"""
    try:
        opts, args = getopt.getopt(argv, "hf:", ["cfg="])
    except getopt.GetoptError:
        print 'script_name.pyc --cfg=<Path to config file>'
        sys.exit(2)
    if not argv:
        print 'script_name.pyc --cfg=<Path to config file>'
        print args
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print 'script_name.py --cfg=<Path to config file>'
            sys.exit(2)
        elif opt in ("-f", "--cfg"):
            return arg.strip()