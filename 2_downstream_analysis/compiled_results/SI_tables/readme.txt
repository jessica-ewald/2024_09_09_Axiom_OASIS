Each row in each file describes the assay activity concentration for each active compound. All files have these columns:

OASIS_ID
    OASIS_ID for the compound

Compound_name:
    The "PREFERRED_NAME" field from CompToxDashboard for the compound

POD_um:
    POD = 'point of departure', and it is the concentration (in micromolar units) at which the dose-response curve surpasses the 95th percentile of the controls. 
    It is somewhat analogous to the AC50, however AC50 is defined relative to the maximal response (top) whereas the POD is relative to the controls (bottom). 
    AC50 is ill-defined when the maximal response is not reached, ie. the concentration range is not high enough or the endpoint has no plateau. 

POD_um_l:
    An approximation of the lower 95% CI of the POD, assessed with a liklihood slice (https://kingaa.github.io/sbied/pfilter/Q_slice.html).

POD_um_u:
    An approximation of the upper 95% CI of the POD, assessed with a liklihood slice (https://kingaa.github.io/sbied/pfilter/Q_slice.html).

Assay_endpoint:
    The specific assay endpoint that we were measuring activity in. 
    The cell count, ldh, and mt assays have only one endpoint.
    For Cell Painting we assessed activity for either all morphological features (global Mahalanobis distance, or gmd) or for subsets of morphological features. 
    In this case, the 'assay_endpoint' column describes the subset of features corresponding to the POD.


The Cell Painting files have the additional column.

Bioactivity_POD:
    In the case that there are multiple PODs for a single compound (ie. activity detected for multiple categories), the overall POD used for downstread analysis is defined as the lowest one. 
    We refer to this POD as the 'bioactivity POD'. 