# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

project = "pipelime"
copyright = "2023, Eyecan.ai https://www.eyecan.ai/"
author = "Eyecan.ai"

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = [
    "myst_parser",
    "sphinxcontrib.mermaid",
    "sphinx.ext.autodoc",
    "sphinx.ext.napoleon",
]

# templates_path = ["_templates"]
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store"]
autosummary_generate = True
myst_heading_anchors = 3

# -- Options for HTML output -------------------------------------------

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.
#
html_theme = "sphinx_material"
html_logo = "pipelime_logo.svg"
html_static_path = ["_static"]
html_sidebars = {
    "**": ["logo-text.html", "globaltoc.html", "localtoc.html", "searchbox.html"]
}

# Theme options are theme-specific and customize the look and feel of a
# theme further.  For a list of options available for each theme, see the
# documentation.
# For the icon search for material icons on Google and look in this list
# https://gist.github.com/albionselimaj/14fabdb89d7893c116ee4b48fdfdc7ae
# if there's a valid code for your choice
html_theme_options = {
    "base_url": "https://www.eyecan.ai",
    "repo_url": "https://github.com/eyecan-ai/pipelime-python/",
    "repo_name": "Check pipelime on github!",
    "repo_type": "github",
    "html_minify": True,
    "touch_icon": "pipelime_logo.svg",
    "css_minify": True,
    "nav_title": "pipelime",
    "nav_links": [
        {
            "href": "https://www.eyecan.ai",
            "internal": False,
            "title": "eyecan.ai",
        },
    ],
    "globaltoc_depth": 1,
    "color_primary": "purple",
    "color_accent": "lime",
    # Visible levels of the global TOC; -1 means unlimited
    "globaltoc_depth": 1,
    # If False, expand all TOC entries
    "globaltoc_collapse": False,
    # If True, show hidden TOC entries
    "globaltoc_includehidden": False,
}
