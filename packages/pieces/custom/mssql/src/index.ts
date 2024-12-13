
    import { createPiece, PieceAuth } from "@activepieces/pieces-framework";
    
    export const mssql = createPiece({
      displayName: "Mssql",
      auth: PieceAuth.None(),
      minimumSupportedRelease: '0.36.1',
      logoUrl: "https://cdn.activepieces.com/pieces/mssql.png",
      authors: [],
      actions: [],
      triggers: [],
    });
    