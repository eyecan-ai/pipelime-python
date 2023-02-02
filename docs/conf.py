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
    "sphinx_immaterial",
    "sphinx_immaterial.apidoc.python.apigen",
]

# templates_path = ["_templates"]
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store"]
autosummary_generate = True
myst_heading_anchors = 3

# -- Options for HTML output -------------------------------------------

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.
#
html_theme = "sphinx_immaterial"
html_title = "pipelime"
html_logo = "pipelime_wireframe.svg"
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
    "repo_url": "https://github.com/eyecan-ai/pipelime-python/",
    "repo_name": "Check pipelime on github!",
    "repo_type": "github",
    "icon": {
        "repo": "fontawesome/brands/github",
        "edit": "material/file-edit-outline",
    },
    "palette": [
        {
            "media": "(prefers-color-scheme: light)",
            "scheme": "default",
            "primary": "green",
            "accent": "green",
            "toggle": {
                "icon": "material/weather-night",
                "name": "Switch to dark mode",
            },
        },
        {
            "media": "(prefers-color-scheme: dark)",
            "scheme": "slate",
            "primary": "green",
            "accent": "green",
            "toggle": {
                "icon": "material/weather-sunny",
                "name": "Switch to light mode",
            },
        },
    ],
    "font": {
        "text": "Roboto",  # used for all the pages' text
        "code": "Roboto Mono",  # used for literal code blocks
    },
    # If False, expand all TOC entries
    "globaltoc_collapse": False,
    # If True, show hidden TOC entries
    "globaltoc_includehidden": False,
    # To investigate
    "features": [
        # "navigation.expand",
        # "navigation.tabs",
        # "toc.integrate",
        # "navigation.sections",
        # "navigation.instant",
        # "header.autohide",
        # "navigation.top",
        "navigation.tracking",
        # "search.highlight",
        "search.share",
    ],
    "version_dropdown": True,
    "toc_title_is_page_title": True,
}
