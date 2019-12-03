package edu.utexas.tacc.tapis.files.api.utils;

import edu.utexas.tacc.tapis.shared.utils.TapisUtils;


/**
   TapisResponse is a generic class representing the 4 part response that Tapis returns.
   The type <T> represents the type of the `result` field. It is necessary to extend this class with
   the response type given, the swagger OpenAPI annotations do NOT support having a generic as the return
   content.

   Example:

   private static class FileListingResponse extends TapisResponse<List<FileInfo>>{}

   @GET
   @ApiResponses(value = {
        @ApiResponse(
            responseCode = "200",
            description = "A list of files",
            content = @Content(schema = @Schema(implementation = FileListingResponse.class))
        )
   })
   public listFiles() {
      // do the file listing...
      listing = getlisting();
      TapisResponse<List<FileInfo>> resp = TapisResponse.createSuccessResponse(listing);
      return Response.ok(resp).build();
   }



   This ensures that the return type of the `result` field will be an array of FileInfo objects and is properly
   picked up by the OpenAPI generators.

 */
public class TapisResponse<T> {

    private String status = "success";
    private String message = "ok";
    private T result;
    private final String version = TapisUtils.getTapisVersion();

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public T getResult() {
        return result;
    }

    public void setResult(T result) {
        this.result = result;
    }

    public String getVersion() {
        return  version;
    }

    public static <T> TapisResponse<T> createSuccessResponse(String msg, T payload) {
        TapisResponse<T> resp = new TapisResponse<>();
        resp.setResult(payload);
        resp.setMessage(msg);
        return resp;
    }

    public static <T> TapisResponse<T> createSuccessResponse(T payload) {
        TapisResponse<T> resp = new TapisResponse<>();
        resp.setResult(payload);
        return resp;
    }

    public static TapisResponse createErrorResponse(String msg) {
        TapisResponse<String> resp = new TapisResponse<>();
        resp.setStatus("error");
        resp.setMessage(msg);
        return resp;
    }

}
