$(document).ready(function () {
    $('#dataTable').DataTable({
        language: {
            searchBuilder: {
                title: ''
            }
        },
        dom: 'QfBrtipRS',
        buttons: [
            'csv', 'excel', 'pdf'
        ],
        // responsive: true,
        fixedHeader: true,
        colReorder: true,
    });
});
