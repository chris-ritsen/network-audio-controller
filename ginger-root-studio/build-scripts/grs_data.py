# -*- coding: utf-8 -*-
"""Ginger Root Studio - palette, collection and print data (single source of truth).

All values transcribed from the reconciled handoff (Appendix B / C / main prompt).
"""

PALETTE = [
    ("Warm Cream", "#F7EEDC"),
    ("Old Parchment", "#E9D8B8"),
    ("Soft Oat", "#D8C5A3"),
    ("Warm Taupe", "#9B8064"),
    ("Soft Cocoa", "#76563F"),
    ("Deep Olive", "#596044"),
    ("Soft Charcoal", "#4B4940"),
    ("Sage", "#A5AA82"),
    ("Moss", "#7C865C"),
    ("Jade", "#61765A"),
    ("Olive Leaf", "#85845A"),
    ("Deep Garden Green", "#3F5540"),
    ("Peach", "#F2B18F"),
    ("Apricot", "#E99A68"),
    ("Warm Coral", "#D96F52"),
    ("Terracotta Rose", "#B85E49"),
    ("Dusty Blush", "#D99B8A"),
    ("Warm Berry", "#A94D3D"),
    ("Buttercream", "#EED7A4"),
    ("Warm Gold", "#D5A04B"),
    ("Ochre", "#C28A3E"),
    ("Antique Mustard", "#B98232"),
    ("Blue Willow", "#879BA0"),
]
HEX = dict(PALETTE)

PALETTE_GROUPS = [
    ("Neutrals", ["Warm Cream", "Old Parchment", "Soft Oat", "Warm Taupe", "Soft Cocoa", "Deep Olive", "Soft Charcoal"]),
    ("Botanical greens", ["Sage", "Moss", "Jade", "Olive Leaf", "Deep Garden Green"]),
    ("Warm florals", ["Peach", "Apricot", "Warm Coral", "Terracotta Rose", "Dusty Blush", "Warm Berry"]),
    ("Golden notes", ["Buttercream", "Warm Gold", "Ochre", "Antique Mustard"]),
    ("One small blue", ["Blue Willow"]),
]

SUBSETS = [
    ("Everyday botanical", ["Warm Cream", "Jade", "Sage", "Peach", "Blue Willow", "Soft Cocoa"]),
    ("Spring / summer", ["Warm Cream", "Peach", "Warm Coral", "Buttercream", "Moss", "Blue Willow"]),
    ("Autumn", ["Old Parchment", "Ochre", "Terracotta Rose", "Warm Berry", "Olive Leaf", "Soft Cocoa"]),
    ("Christmas", ["Warm Cream", "Deep Garden Green", "Warm Berry", "Warm Gold", "Soft Cocoa"]),
    ("Quiet winter", ["Warm Cream", "Soft Oat", "Sage", "Blue Willow", "Soft Cocoa"]),
]

# The earlier standalone Christmas branch palette (NOT part of the .ase file)
OLD_CHRISTMAS_PALETTE = [
    ("Ivory", "#F5F0E5"), ("Oat", "#D9C9AB"), ("Gold", "#B59652"), ("Evergreen", "#243F35"),
    ("Cranberry", "#88444C"), ("Midnight", "#25344D"), ("Candlelight", "#D5B779"),
]

ROLE_ORDER = ["Hero", "Secondary", "Coordinate", "Blender", "Micro"]

def P(pid, name, role, motifs, repeat, decision, scale):
    return dict(id=pid, name=name, role=role, motifs=motifs, repeat=repeat, decision=decision, scale=scale)

S_HERO = "3-5 in motifs; 18-24 in repeat study"
S_SEC = "1.5-3 in motifs; 12-16 in repeat study"
S_CO = "0.4-1.2 in motifs; 6-12 in repeat study"
S_BL = "0.2-0.6 in rhythm; 4-8 in repeat study"
S_MI = "0.15-0.35 in marks; 4-6 in repeat study"

COLLECTIONS = [
    dict(
        code="HG", name="Harvest Garden", family="Botanical & Seasonal", season="Autumn",
        story="The garden at the end of summer: flowers still open, seed heads gathering, and leaves beginning to turn. Warm coral and ochre carry autumn while cream and peach keep the studio palette luminous.",
        palette=["Warm Cream", "Warm Coral", "Terracotta Rose", "Ochre", "Moss", "Deep Olive", "Soft Cocoa", "Peach"],
        signature="Dahlia petals, oak lobes, and fine curling stems; warm, gathered abundance.",
        library="One front-facing dahlia, one side flower, one opening bud, three oak leaves, two stem curves, a berry branch, and an acorn.",
        pairing="Autumn Garden + Oak & Vine + Acorn Dot",
        steps=["Map the dahlia as nested petal rings around an uneven center.",
               "Draw oak lobes in silhouette, then add only the strongest veins.",
               "Build one floral cluster and one gathered-object cluster before arranging the repeat."],
        prints=[
            P("HG01", "Autumn Garden", "Hero", "Dahlias, chrysanthemums and climbing foliage", "Flowing half-drop; large blooms interrupt smaller clusters", "Vary the petal rings so the flowers feel hand drawn.", S_HERO),
            P("HG02", "Gathered Garden", "Hero", "Flowers, acorns, berries, seed pods and branches", "Loose narrative toss with a few open pockets", "Keep the small objects subordinate to the flower groups.", S_HERO),
            P("HG03", "Dahlia Study", "Secondary", "Individual dahlias with short leafy stems", "Airy offset bloom repeat", "Use three flower angles rather than one repeated circle.", S_SEC),
            P("HG04", "Oak & Vine", "Secondary", "Oak leaves, curling vines and berries", "Diagonal trailing branches", "Let the oak silhouette read before adding vein detail.", S_SEC),
            P("HG05", "Seed & Stem", "Coordinate", "Small pods on fine stems", "Open sprig scatter", "Draw one closed pod and one opened pod.", S_CO),
            P("HG06", "Fallen Leaves", "Coordinate", "Three small leaf silhouettes", "Multidirectional toss", "Change angle and spacing more than color.", S_CO),
            P("HG07", "Berry Vine", "Coordinate", "Small berry clusters on curved stems", "Gentle linked diagonal trail", "Keep berry sizes small and the stems thin.", S_CO),
            P("HG08", "Orchard Sprig", "Coordinate", "Small apples, leaves and short twigs", "Spacious alternating sprigs", "Added to complete the ten-print lineup; keep it quieter than the story hero.", S_CO),
            P("HG09", "Garden Trellis", "Blender", "Thin interlaced botanical curves", "Low-contrast open lattice", "Use one continuous curve family with broad background gaps.", S_BL),
            P("HG10", "Acorn Dot", "Micro", "Tiny acorns, dots and occasional leaves", "Even micro scatter with slight irregularity", "Simplify the cup and nut to two readable shapes.", S_MI),
        ]),
    dict(
        code="CG", name="Christmas at the Garden", family="Botanical & Seasonal", season="Christmas",
        story="A festive cottage garden translated into flowers and greenery. Poinsettias, camellias, berries and pinecones sit on warm cream, connecting Christmas to the studio's year-round botanical identity.",
        palette=["Warm Cream", "Old Parchment", "Warm Berry", "Terracotta Rose", "Jade", "Deep Garden Green", "Warm Gold", "Soft Cocoa"],
        signature="Cream grounds, winter flowers, and small warm-red accents; the botanical Christmas branch of the brand.",
        library="A poinsettia cluster, a rounded camellia, two holly leaves, a pine sprig, a pinecone, a berry branch, and one fine ribbon line.",
        pairing="Christmas Garden + Camellia Noel + Antique Stripe",
        steps=["Separate pointed poinsettia bracts from rounded camellia petals.",
               "Draw holly and pine with the same delicate contour weight.",
               "Test the cream-ground hero first, then derive the sprigs and tiny motifs."],
        prints=[
            P("CG01", "Christmas Garden", "Hero", "Poinsettias, camellias, evergreen and berries", "Open cream-ground floral half-drop", "Let broad flower shapes lead; use gold only in small centers.", S_HERO),
            P("CG02", "Winter Berry", "Hero", "Evergreen branches, berries, flowers and pinecones", "Sweeping branch clusters with alternating diagonals", "Build rhythm with branch direction rather than dense filler.", S_HERO),
            P("CG03", "Camellia Noel", "Secondary", "Camellias and short holly branches", "Medium-scale floral toss", "Reuse the Jade Garden petal language in a festive colorway.", S_SEC),
            P("CG04", "Evergreen Study", "Secondary", "Pine branches, cones and berries", "Offset botanical specimens", "Keep cones lighter and smaller than the needles around them.", S_SEC),
            P("CG05", "Holly Vine", "Coordinate", "Small holly leaves and berries", "Fine trailing vine", "Use a restrained leaf-and-berry rhythm.", S_CO),
            P("CG06", "Tiny Poinsettia", "Coordinate", "Simplified small poinsettia flowers", "Spacious all-over scatter", "Reduce the flower to a clear pointed silhouette.", S_CO),
            P("CG07", "Berry Sprig", "Coordinate", "Small paired berry branches", "Gentle alternating toss", "Leave the cream background dominant.", S_CO),
            P("CG08", "Pinecone Sprig", "Coordinate", "Tiny cones with a few pine needles", "Open directional sprigs", "Added to complete the ten-print lineup; simplify cone scales.", S_CO),
            P("CG09", "Antique Stripe", "Blender", "Fine uneven gold and garden-green lines", "Narrow vertical stripe", "Keep the stripe lighter than the florals.", S_BL),
            P("CG10", "Snowflake & Berry", "Micro", "Small branching snow marks and tiny berries", "Quiet micro dot rhythm", "Use spare linework and very few snow shapes.", S_MI),
        ]),
    dict(
        code="WG", name="Winter Garden", family="Botanical & Seasonal", season="Winter",
        story="A quiet garden after frost. Bare branches, pale camellias and small berries create a winter story that can continue after Christmas. Warm cream and oat hold the warmth; Blue Willow supplies the cool breath.",
        palette=["Warm Cream", "Soft Oat", "Sage", "Jade", "Deep Garden Green", "Blue Willow", "Dusty Blush", "Soft Cocoa"],
        signature="Bare twig structure, pale blooms, and generous space; subdued winter rather than festive Christmas.",
        library="Three camellia angles, a bare branching twig, two pinecones, a fine pine sprig, a closed bud, and a small berry cluster.",
        pairing="Winter Camellia + Pine & Cone + Snowberry",
        steps=["Map the branch forks before adding flowers or berries.",
               "Use the camellia contour with lighter fills and fewer petal shadows.",
               "Alternate full flower groups with quiet bare-branch areas."],
        prints=[
            P("WG01", "Winter Camellia", "Hero", "Pale camellias, evergreen leaves and berries", "Spacious floral half-drop", "Give petals broad light areas and keep berries muted.", S_HERO),
            P("WG02", "Frosted Garden", "Hero", "Bare branches, berries, tiny flowers and pine", "Open directional branch network", "Let unfilled branches supply the winter character.", S_HERO),
            P("WG03", "Camellia Study", "Secondary", "Single pale camellia stems", "Airy offset repeat", "Use one full bloom and two side views.", S_SEC),
            P("WG04", "Pine & Cone", "Secondary", "Short pine branches and cones", "Alternating botanical sprigs", "Reduce needle density so the drawing stays soft.", S_SEC),
            P("WG05", "Berry Vine", "Coordinate", "Small winter berries and bare stems", "Light winding trail", "Use the stems as the main pattern rhythm.", S_CO),
            P("WG06", "Winter Leaves", "Coordinate", "Small oval and pointed leaves", "Quiet tossed repeat", "Keep the leaves thinly painted and lightly outlined.", S_CO),
            P("WG07", "Tiny Pine", "Coordinate", "Very small pine tips", "Open staggered scatter", "Draw a few needle groups instead of every needle.", S_CO),
            P("WG08", "Branch & Bud", "Coordinate", "Short bare twigs with closed buds", "Minimal alternating sprigs", "Preserve gaps between the bud and twig forks.", S_CO),
            P("WG09", "Winter Lattice", "Blender", "Thin twig-like crossing lines", "Low-contrast loose grid", "Avoid dark intersections that turn the pattern into a check.", S_BL),
            P("WG10", "Snowberry", "Micro", "Tiny berries and small dots", "Soft nearly regular micro repeat", "Keep the berries smaller than the coordinates.", S_MI),
        ]),
    dict(
        code="SG", name="The Secret Garden", family="Botanical & Seasonal", season="Spring",
        story="The first flowers unfolding inside a sheltered garden. Camellias and peonies share space with small birds and butterflies; curved stems imply paths and discoveries without illustrating a literal garden map.",
        palette=["Warm Cream", "Peach", "Dusty Blush", "Buttercream", "Sage", "Jade", "Warm Coral", "Blue Willow"],
        signature="Layered rounded flowers, curling buds, and small hidden companions.",
        library="One loose peony, two camellias, three wildflower stems, a closed bud, two bird poses, and a butterfly.",
        pairing="Secret Garden + Wildflower Meadow + Petal Scatter",
        steps=["Contrast loose peony edges with the clearer camellia petal layers.",
               "Sketch one bird as a simple body-and-wing silhouette.",
               "Thread the smaller stems through two hero flower clusters."],
        prints=[
            P("SG01", "Secret Garden", "Hero", "Peonies, camellias, wildflowers and vines", "Flowing layered half-drop", "Keep a few untouched cream pockets around the large blooms.", S_HERO),
            P("SG02", "Spring Song", "Hero", "Flowers, perched birds and butterflies", "Airy narrative toss", "Let the birds occupy spaces between flowers rather than sitting on top of them.", S_HERO),
            P("SG03", "Peony Study", "Secondary", "Loose peony blossoms and leaves", "Offset single-stem study", "Vary the ruffled outer contour without filling every petal.", S_SEC),
            P("SG04", "Wildflower Meadow", "Secondary", "Several fine wildflower stems", "Light mixed-direction scatter", "Keep flower heads smaller than those in the heroes.", S_SEC),
            P("SG05", "Butterfly Garden", "Coordinate", "Small butterflies and a few buds", "Quiet spaced toss", "Use simple wing silhouettes with minimal markings.", S_CO),
            P("SG06", "Little Leaves", "Coordinate", "Paired spring leaves", "Open offset repeat", "Use small changes in leaf angle.", S_CO),
            P("SG07", "Vine & Bud", "Coordinate", "Curved stems with opening buds", "Gentle continuous trail", "Keep each stem shallow enough to remain graceful at small scale.", S_CO),
            P("SG08", "Tiny Florals", "Coordinate", "Small five-petal blooms", "Simple small scatter", "Use one petal family and two sizes.", S_CO),
            P("SG09", "Garden Lattice", "Blender", "Fine arches and crossing vine curves", "Pale regular lattice", "Leave intersections light and uncluttered.", S_BL),
            P("SG10", "Petal Scatter", "Micro", "Loose tiny petals", "Soft micro toss", "Let the background dominate and avoid obvious rows.", S_MI),
        ]),
    dict(
        code="GP", name="The Garden Party", family="Botanical & Seasonal", season="Summer",
        story="The most cheerful expression of the studio palette: ripe strawberries, open flowers, garden birds and butterflies. Peach, coral and buttercream feel warm and fresh, with Blue Willow used as a small contrasting note.",
        palette=["Warm Cream", "Peach", "Warm Coral", "Buttercream", "Jade", "Sage", "Blue Willow", "Warm Gold"],
        signature="Lighthearted garden abundance with fine linework and clear botanical shapes.",
        library="Two summer flowers, one airy wildflower, a strawberry with leaves, a butterfly, a small bird, and three loose petals.",
        pairing="Garden Party + Strawberry Vine + Garden Stripe",
        steps=["Draw the strawberry as a tapered form with sparse seed marks.",
               "Use flowers with open centers to distinguish summer from the layered spring blooms.",
               "Build one full garden group and one airy meadow group."],
        prints=[
            P("GP01", "Garden Party", "Hero", "Open flowers, strawberries, butterflies and birds", "Buoyant botanical story half-drop", "Keep fruit and birds smaller than the statement flowers.", S_HERO),
            P("GP02", "Summer Meadow", "Hero", "Loose wildflowers and grasses", "Airy multidirectional meadow", "Allow long stems and broad areas of cream.", S_HERO),
            P("GP03", "Strawberry Vine", "Secondary", "Strawberries, leaves and small flowers", "Curving fruiting vine", "Use a few berries at different stages instead of repeated identical fruit.", S_SEC),
            P("GP04", "Summer Bloom Study", "Secondary", "Open summer blossoms", "Spaced medium flower scatter", "Vary centers while preserving the same petal language.", S_SEC),
            P("GP05", "Tiny Strawberry", "Coordinate", "Small simplified strawberries", "Light tossed repeat", "Limit seed marks to a few dots.", S_CO),
            P("GP06", "Butterfly", "Coordinate", "Small butterflies in two poses", "Open offset scatter", "Match the wing curves to the flower curves.", S_CO),
            P("GP07", "Wildflower", "Coordinate", "Single tiny flowering stems", "Quiet sprig toss", "Keep stems fine and blooms uncomplicated.", S_CO),
            P("GP08", "Little Leaf", "Coordinate", "Small summer leaves", "Gentle multidirectional repeat", "Use a clear paired-leaf silhouette.", S_CO),
            P("GP09", "Garden Stripe", "Blender", "Fine softly wavering lines", "Narrow low-contrast stripe", "Let irregular line edges retain a drawn quality.", S_BL),
            P("GP10", "Petal & Dot", "Micro", "Tiny petals and warm dots", "Open micro scatter", "Balance the two shapes without making a strict polka dot.", S_MI),
        ]),
    dict(
        code="JG", name="Jade Garden", family="Botanical & Seasonal", season="Signature / evergreen",
        story="The signature evergreen collection: rounded camellias, flowing vines and small garden companions. Fine lines, warm parchment and rich greens connect Art Nouveau movement with the quiet structure of a collected garden.",
        palette=["Warm Cream", "Old Parchment", "Jade", "Moss", "Dusty Blush", "Warm Gold", "Soft Charcoal", "Blue Willow"],
        signature="The camellia is the signature bloom; S-curves create movement and ginkgo fans provide a restrained structural accent.",
        library="Three camellias: full, half-open and side-facing. Add a bell-shaped flower, two vine curves, a ginkgo leaf, one bird and a dragonfly.",
        pairing="Jade Garden + Camellia Study + Jade Lattice",
        steps=["Begin with an uneven oval and overlapping rounded outer petals.",
               "Add smaller inner petal groups; keep the center open enough to read.",
               "Build an S-curve vine linking two differently angled camellias."],
        prints=[
            P("JG01", "Jade Garden", "Hero", "Camellias, bell-shaped flowers, vines and foliage", "Rich but open half-drop", "Use three camellia angles and flowing S-curves as the structural spine.", S_HERO),
            P("JG02", "Garden Companions", "Hero", "Flowers, birds, butterflies and dragonflies", "Spacious narrative toss", "Keep companions smaller than the blooms and faces understated.", S_HERO),
            P("JG03", "Bloom Study", "Secondary", "Small mixed blossoms and short leaves", "Compact medium floral clusters", "Use bell flowers and open blossoms so this differs from Camellia Study.", S_SEC),
            P("JG04", "Camellia Study", "Secondary", "Single camellias and buds", "Airy offset stem repeat", "Reduce each bloom to clear layered petals.", S_SEC),
            P("JG05", "Vine & Stem", "Coordinate", "Thin stems and narrow leaves", "Graceful diagonal trail", "Let the curve do more work than interior detail.", S_CO),
            P("JG06", "Ginkgo Leaf", "Coordinate", "Small fan-shaped ginkgo leaves", "Quiet alternating toss", "Vary the notch and fan width without making a geometric icon.", S_CO),
            P("JG07", "Cloud Garden", "Coordinate", "Soft layered cloud-like curves", "Open rounded ornamental rows", "Use an original curved structure with only a few repeated contours.", S_CO),
            P("JG08", "Tiny Bud", "Coordinate", "Small closed flower buds", "Loose staggered scatter", "Simplify each bud to two or three lines and a light fill.", S_CO),
            P("JG09", "Jade Lattice", "Blender", "Thin garden-inspired crossing curves", "Low-contrast open lattice", "Maintain consistent spacing and delicate intersections.", S_BL),
            P("JG10", "Seed & Petal", "Micro", "Tiny seeds and petals", "Calm micro scatter", "Keep one slightly elongated mark family throughout.", S_MI),
        ]),
    # ---------------------------------------------------------------- Sacred Seasons
    dict(
        code="OH", name="O Holy Night", family="Sacred Seasons", season="Christmas Eve",
        story="Christmas Eve, the birth of Christ, and warm light entering the night. This is the existing twelve-print expanded collection, carried into the Ginger Root palette while retaining its nativity, angel, botanical and quiet-coordinate stories.",
        palette=["Warm Cream", "Old Parchment", "Deep Garden Green", "Deep Olive", "Warm Berry", "Warm Gold", "Soft Cocoa", "Blue Willow"],
        signature="The Star of Bethlehem, fine gold-colored rays, olive sprigs, herald trumpets, bells and candlelight.",
        library="A small manger and shelter, an angel wing, a herald trumpet, a church bell, a taper candle, an olive sprig and an eight-pointed star.",
        pairing="The Holy Night + Bethlehem Starlight + Manger Linen",
        steps=["Draw the shelter and star as the central nativity structure.",
               "Give wings, trumpets and bells clear silhouettes before ornament.",
               "Develop one open cream-ground print beside the richer night-ground hero."],
        prints=[
            P("OH01", "The Holy Night", "Hero", "A small Holy Family beside a manger; a simple shelter arch; the eight-pointed Star of Bethlehem; olive sprigs; narrow rays of golden light.", "Build a narrative half-drop from three slightly different nativity vignettes. Let olive branches link the groups while deep night-ground space keeps each scene readable.", "Use the star and radiating arch as the strongest shapes. Reduce faces and folds to a few graceful marks so the image still reads when printed smaller.", "Main vignette 3.5-5 in; suggested repeat 16 x 20 in"),
            P("OH02", "Heralds of Glory", "Hero", "Three robed angel poses; feathered wings; long herald trumpets; blank curling ribbons; small stars; restrained botanical flourishes.", "Alternate left- and right-facing angels in a half-drop. Let trumpet diagonals and ribbon curves lead the eye between figures. Keep open cream pockets around the wings.", "The herald trumpet supplies the visual motion. The angel should feel like an elegant storybook figure, with believable wings and a simple, quiet expression.", "Angel height 3-4.5 in; suggested repeat 16 x 16 in"),
            P("OH03", "Heaven and Nature", "Hero", "Holly leaves and berries; fir tips; olive sprigs; cream hellebore blossoms; tiny gold-colored starbursts.", "Interlock rounded bouquets on diagonal paths. Rotate some clusters for a multidirectional botanical repeat; vary flower and foliage sizes so the field has movement.", "This is the abundant hero. Use clusters of leaves and blossoms to create fullness while preserving dark gaps that keep it from becoming a solid floral mass.", "Bouquet width 2.5-4 in; suggested repeat 12 x 16 in"),
            P("OH04", "Bethlehem at Dusk", "Secondary", "Simple stone houses; flat roofs and occasional domes; arched windows; cypress and olive trees; tiny gold stars.", "Arrange three village clusters in softly staggered horizontal bands. Vary the roofline and leave strips of night sky between rows. The houses remain directional.", "Keep the architecture suggestive of Bethlehem while using the drawing style of old European Christmas cards. A few illuminated windows create the emotional focus.", "Village cluster 2-3 in; suggested repeat 12 x 12 in"),
            P("OH05", "A Still Small Light", "Secondary", "Slim candles; modest brass holders; three flame shapes; fine radiating arcs; olive sprigs; a few tiny gold dots.", "Use a spacious tossed repeat with candles at slightly different heights. Keep their angles close to upright and scatter olive sprigs between the holders.", "Draw candlelight with a small gold flame and one or two crisp arcs. The warm feeling should come from spacing and color, with most of the paper left quiet.", "Candle height 1.5-2.5 in; suggested repeat 10 x 12 in"),
            P("OH06", "Gifts of the Magi", "Secondary", "A lidded gold casket; an incense vessel with a curling wisp; a narrow myrrh jar; guiding stars; occasional olive sprigs.", "Make three small gift clusters and alternate them in an offset repeat. Turn the vessels slightly and let the incense curves connect the spaces between groups.", "Separate the three gifts through silhouette: a low casket, a wider incense vessel, and a slender jar. Repeat the same few ornament marks to keep them related.", "Vessel height 1-2 in; suggested repeat 12 x 12 in"),
            P("OH07", "Come and Adore", "Secondary", "Open olive-and-holly arches; delicate hanging lanterns; small candles; narrow ribbon tails; sparse gold leaf ticks.", "Repeat gently offset arches with alternating lantern and candle centers. Leave the center of each arch open, and use only a few ribbon accents across the tile.", "Use open arches instead of closed wreaths. That gives the motif a sense of arrival and keeps the print distinct from the collection's abundant botanical hero.", "Arch height 2-3 in; suggested repeat 12 x 16 in"),
            P("OH08", "Bells of Peace", "Secondary", "Antique flared church bells; visible small clappers; narrow berry-red bows; fir sprigs; single and paired bell arrangements.", "Toss the bell groups at restrained alternating angles. Pair some bells and leave others single; place bows and fir tips so each group retains a clear silhouette.", "The bell shape should feel old and ceremonial. Give it a wide mouth and a small clapper, then use only a few engraved lines to suggest the metal surface.", "Bell group 1.25-2 in; suggested repeat 10 x 12 in"),
            P("OH09", "Bethlehem Starlight", "Coordinate", "Two sizes of eight-pointed stars; a few cream pinpoints; one fine outline variation. Keep the star family consistent with the nativity hero.", "Use an open offset scatter with a steady rhythm. Separate the larger stars and let smaller pinpoints break the grid without creating a dense night-sky field.", "The repeated stars represent the Star of Bethlehem symbolically. Use eight points and a slightly longer vertical ray, with enough spacing for this to work as a quiet coordinate.", "Star width 0.2-0.45 in; suggested repeat 6 x 6 in"),
            P("OH10", "Gloria Garland", "Coordinate", "A few curved feather strokes; shallow scallops; tiny paired olive leaves; delicate gold arcs.", "Link feather curves into gently repeating scallop rows. Offset alternate rows and keep the connecting leaves tiny so the design reads as an ornamental texture.", "This coordinate should echo the movement of an angel wing while remaining abstract. Reduce feather detail to two or three strokes that survive at small size.", "Scallop span 0.5-0.8 in; suggested repeat 6 x 6 in"),
            P("OH11", "Carol Ribbon", "Coordinate", "Thin undulating ribbons; alternating gold and cream lines; very occasional tiny star pulses; a few simple radiating marks.", "Make a restrained lengthwise stripe with a shallow wave. Stagger the rare star accents across adjacent lines. Check that the flow continues cleanly at the tile boundaries.", "The line rhythm is the reference to song. Keep the stripe quieter than the bell and trumpet prints, with only enough star accents to connect it to the collection.", "Stripe spacing 0.2-0.4 in; suggested repeat 6 x 8 in"),
            P("OH12", "Manger Linen", "Coordinate", "Broken cream crosshatching; tiny straw-like strokes; open basketweave squares; subtle alternating parchment and cream marks.", "Build a small geometric texture with slight variation in the broken marks. Keep an even overall density and very low contrast so it reads almost as a solid.", "Make the linen effect through drawn marks rather than a photographic fabric surface. It should provide warmth and visual rest without adding another narrative motif.", "Texture mark 0.06-0.15 in; suggested repeat 4 x 4 in"),
        ]),
    dict(
        code="CF", name="Come Thou Fount", family="Sacred Seasons", season="Spring / renewal",
        story="Renewal and mercy translated into a meadow gathered around flowing water. The stems behave like tributaries: they curve, meet, and open into small flowers. A fountain offers one quiet story motif.",
        palette=["Warm Cream", "Peach", "Buttercream", "Jade", "Sage", "Blue Willow", "Warm Coral", "Warm Gold"],
        signature="Flowing stem lines, small meadow flowers and a gently falling water arc.",
        library="Three wildflower stems, one small bird, one butterfly, a shallow water arc, a fountain bowl, and several loose petals.",
        pairing="Come Thou Fount + Wildflower Study + Flowing Vine",
        steps=["Draw a long stream-like S-curve, then let stems grow from its bends.",
               "Simplify a fountain to a bowl, pedestal and thin water arcs.",
               "Keep meadow clusters small enough for the flowing structure to remain visible."],
        prints=[
            P("CF01", "Come Thou Fount", "Hero", "Wildflowers and stems arranged like a flowing stream", "Botanical river half-drop", "Let the main stem path stay visible through the flowers.", S_HERO),
            P("CF02", "Streams of Mercy", "Hero", "Flowers, water curves, birds and butterflies", "Open narrative flow", "Use very few birds so the water rhythm leads.", S_HERO),
            P("CF03", "Wildflower Study", "Secondary", "Fine flowering stems", "Airy specimen scatter", "Draw three recognizable flower-head shapes.", S_SEC),
            P("CF04", "Fountain Garden", "Secondary", "A vintage garden fountain and small flowers", "Spaced fountain medallions", "Keep the fountain simple and botanicals light.", S_SEC),
            P("CF05", "Little Wildflowers", "Coordinate", "Tiny single blossoms", "Open tossed repeat", "Use two small bloom shapes and a narrow stem.", S_CO),
            P("CF06", "Water & Vine", "Coordinate", "Short water arcs and leafy curves", "Winding low-density trail", "Let the lines echo without turning into a dense wave pattern.", S_CO),
            P("CF07", "Butterfly Meadow", "Coordinate", "Small butterflies and buds", "Light offset scatter", "Leave broad spaces around the wings.", S_CO),
            P("CF08", "Tiny Petals", "Coordinate", "Small falling petals", "Quiet multidirectional toss", "Vary orientation rather than silhouette.", S_CO),
            P("CF09", "Flowing Vine", "Blender", "Single shallow S-curves", "Pale continuous flowing lines", "Check the line joins at the tile boundaries.", S_BL),
            P("CF10", "Rain & Petal", "Micro", "Tiny drops, petals and dots", "Soft micro scatter", "Keep drops small and irregularly spaced.", S_MI),
        ]),
    dict(
        code="SP", name="His Eye Is on the Sparrow", family="Sacred Seasons", season="Spring / birds",
        story="Care, protection and quiet joy expressed through small birds held within flowering branches. The collection should feel observant and tender, with natural bird silhouettes and a sheltering botanical structure.",
        palette=["Warm Cream", "Peach", "Blue Willow", "Sage", "Jade", "Warm Coral", "Buttercream", "Soft Cocoa"],
        signature="Sparrow profiles and branch arches; a calm bird story that can also adapt for children.",
        library="A perched sparrow, a turned sparrow, one open wing, a flowering branch, a berry sprig, a feather and two small leaves.",
        pairing="His Eye Is on the Sparrow + Sparrow Study + Feather & Leaf",
        steps=["Block in a sparrow with an oval body, small head and short conical beak.",
               "Attach a simple wing shape before adding feather marks.",
               "Use branches to frame birds while leaving their silhouettes clear."],
        prints=[
            P("SP01", "His Eye Is on the Sparrow", "Hero", "Sparrows in flowering branches", "Sheltering botanical half-drop", "Give each bird a clear perch and space around its head.", S_HERO),
            P("SP02", "Under His Wings", "Hero", "Birds and broad sweeping foliage", "Open wing-like branch arcs", "Suggest shelter through the composition rather than literal symbols.", S_HERO),
            P("SP03", "Sparrow Study", "Secondary", "Perched sparrows in several poses", "Airy bird-and-branch repeat", "Vary head direction while keeping bird proportions consistent.", S_SEC),
            P("SP04", "Garden Birds", "Secondary", "Small birds, flowers and berries", "Medium narrative clusters", "Use fewer flowers than in the hero.", S_SEC),
            P("SP05", "Little Sparrow", "Coordinate", "Simple small bird profiles", "Spaced tossed repeat", "Keep the beak, wing and tail readable at small scale.", S_CO),
            P("SP06", "Bird & Vine", "Coordinate", "Tiny birds on winding stems", "Loose trailing repeat", "Break the vine with open gaps to avoid visual tangles.", S_CO),
            P("SP07", "Feather & Leaf", "Coordinate", "Small feathers and leaves", "Quiet alternating scatter", "Use related curves but distinguish each silhouette.", S_CO),
            P("SP08", "Berry Branch", "Coordinate", "Small berry twigs", "Open sprig repeat", "Keep twig forks simple.", S_CO),
            P("SP09", "Wing & Vine", "Blender", "Abstract wing curves and fine stems", "Low-contrast scalloped trail", "Reduce feather details to a few light strokes.", S_BL),
            P("SP10", "Tiny Birds", "Micro", "Very small birds and leaves", "Sparse micro repeat", "Remove all detail that disappears at actual size.", S_MI),
        ]),
    dict(
        code="BV", name="Be Thou My Vision", family="Sacred Seasons", season="Ancient ornament",
        story="Guidance and clarity expressed through old botanical ornament. Interlacing vines, oak leaves and small stars create the feeling of an illuminated border; the ornament remains open enough to belong to a garden collection.",
        palette=["Old Parchment", "Deep Garden Green", "Jade", "Warm Gold", "Terracotta Rose", "Soft Cocoa", "Blue Willow"],
        signature="Simple botanical interlace with visible over-and-under logic; oak leaves and restrained stars.",
        library="One open vine knot, two oak leaves, a small rose, an eight-point star, a berry sprig and a narrow ornamental border.",
        pairing="Be Thou My Vision + Oak & Star + Celtic Lattice",
        steps=["Draw the vine route as one uninterrupted line before adding interlace.",
               "Alternate over and under crossings consistently.",
               "Attach oak leaves and small flowers only after the structure reads clearly."],
        prints=[
            P("BV01", "Be Thou My Vision", "Hero", "Interlaced botanical vines, stars and small flowers", "Open ornamental half-drop", "Keep the star a small point of clarity within the vine structure.", S_HERO),
            P("BV02", "Ancient Garden", "Hero", "Vine knots, oak leaves, flowers and berries", "Balanced botanical medallions", "Use fewer crossings than a dense knotwork panel.", S_HERO),
            P("BV03", "Celtic Rose", "Secondary", "Small roses within simple curved ornament", "Airy repeating rose frames", "Keep the rose silhouette stronger than its frame.", S_SEC),
            P("BV04", "Oak & Star", "Secondary", "Oak leaves with small stars", "Spaced diagonal sprigs", "Use a clear leaf lobe shape and restrained star scale.", S_SEC),
            P("BV05", "Vine Knot", "Coordinate", "One simple looping vine", "Open offset knot repeat", "Check that every crossing has consistent over-and-under order.", S_CO),
            P("BV06", "Little Stars", "Coordinate", "Small warm-gold stars", "Quiet staggered scatter", "Use one star family in two sizes.", S_CO),
            P("BV07", "Oak Leaf", "Coordinate", "Small oak leaves", "Gentle tossed repeat", "Simplify the veins and vary the leaf angle.", S_CO),
            P("BV08", "Ancient Bloom", "Coordinate", "Tiny ornamental blossoms", "Open flower scatter", "Use a simple original floral shape rather than copying an emblem.", S_CO),
            P("BV09", "Celtic Lattice", "Blender", "Thin interlaced curves", "Low-contrast open lattice", "Keep the repeat join as clean as the internal crossings.", S_BL),
            P("BV10", "Star & Vine", "Micro", "Tiny stars with short curved marks", "Sparse micro rhythm", "Let parchment dominate.", S_MI),
        ]),
    dict(
        code="GA", name="How Great Thou Art", family="Sacred Seasons", season="Creation / landscape",
        story="Wonder at creation, from a small meadow flower to a distant mountain. Varying scale creates the feeling of a naturalist's notebook, while repeated stems and quiet color keep the landscape elements inside the studio's botanical world.",
        palette=["Warm Cream", "Jade", "Moss", "Blue Willow", "Warm Gold", "Peach", "Terracotta Rose", "Deep Garden Green"],
        signature="Layered mountain silhouettes, airy meadow flowers, and a restrained day-to-night celestial rhythm.",
        library="Two mountain ridges, one conifer, three wildflowers, a small bird, a sun disk, an eight-point star and two leaves.",
        pairing="How Great Thou Art + Mountain Meadow + Mountain Stripe",
        steps=["Draw mountain ridges with distinct silhouettes and very few interior lines.",
               "Keep foreground flowers larger than the distant landscape details.",
               "Arrange landscape groups as separate repeated vignettes with open sky between them."],
        prints=[
            P("GA01", "How Great Thou Art", "Hero", "Mountains, wildflowers, trees, birds, sun and stars", "Spacious naturalist landscape vignettes", "Choose one focal scale per vignette to keep the scene readable.", S_HERO),
            P("GA02", "Creation", "Hero", "Flowers, trees, birds and mountain forms", "Botanical clusters with small landscape inserts", "Let vines connect the different natural elements.", S_HERO),
            P("GA03", "Mountain Meadow", "Secondary", "Small ridges above flowering meadows", "Open repeating landscape bands", "Leave clear space between bands.", S_SEC),
            P("GA04", "Night Sky Garden", "Secondary", "Stars among lightly drawn flowers", "Airy floral and star scatter", "Use stars sparingly so flowers remain recognizable.", S_SEC),
            P("GA05", "Wildflower", "Coordinate", "Single small flowering stems", "Spaced meadow sprigs", "Use a limited bloom family.", S_CO),
            P("GA06", "Mountain Line", "Coordinate", "Short simple ridge silhouettes", "Quiet alternating horizontal repeat", "Avoid large dark masses.", S_CO),
            P("GA07", "Little Stars", "Coordinate", "Tiny gold stars", "Open offset scatter", "Match the stars used in the story hero.", S_CO),
            P("GA08", "Creation Leaves", "Coordinate", "Small leaves from the hero foliage", "Light multidirectional toss", "Keep foliage shapes consistent across the range.", S_CO),
            P("GA09", "Mountain Stripe", "Blender", "Fine repeating ridge-like lines", "Low-contrast shallow zigzag stripe", "Make peaks broad and gentle.", S_BL),
            P("GA10", "Starflower", "Micro", "Tiny flowers with star-like centers", "Quiet micro repeat", "Simplify the flower to a clear five-petal shape.", S_MI),
        ]),
    dict(
        code="IW", name="It Is Well", family="Sacred Seasons", season="Water / peace",
        story="Peace and steadiness expressed through water lilies, willow branches and unhurried ripples. Calm comes from spacing, stable horizontal movement and a restrained palette, with warm cream preventing the water colors from feeling cold.",
        palette=["Warm Cream", "Blue Willow", "Sage", "Jade", "Dusty Blush", "Warm Gold", "Soft Cocoa"],
        signature="Open lily forms, broad pads and long low ripples; the quietest Sacred Seasons collection.",
        library="A water lily from above, a side-facing lily, two lily pads, a reed cluster, a willow branch, a small bird and three ripple curves.",
        pairing="It Is Well + Water Lily Study + Rippling Water",
        steps=["Separate the pointed lily petals from the broad rounded pad silhouette.",
               "Draw ripples as shallow ellipses with deliberate breaks.",
               "Keep lily groups apart so open water is part of the design."],
        prints=[
            P("IW01", "It Is Well", "Hero", "Water lilies and graceful broad leaves", "Slow spacious floating floral repeat", "Allow generous open water between the lily groups.", S_HERO),
            P("IW02", "Still Waters", "Hero", "Lilies, reeds, willow and small birds", "Quiet horizontal story vignettes", "Keep the horizon implied rather than drawing a solid line.", S_HERO),
            P("IW03", "Water Lily Study", "Secondary", "Single lilies and pads", "Airy specimen scatter", "Use both top and side views.", S_SEC),
            P("IW04", "Willow Garden", "Secondary", "Fine drooping willow branches", "Open trailing branch repeat", "Vary the branch lengths while keeping the movement gentle.", S_SEC),
            P("IW05", "Lily Pad", "Coordinate", "Simple small lily pads", "Quiet floating scatter", "Keep the notch and leaf edge clearly drawn.", S_CO),
            P("IW06", "Reeds", "Coordinate", "Small reed groups", "Spacious nearly upright sprigs", "Use a few slender stems per group.", S_CO),
            P("IW07", "Water & Leaf", "Coordinate", "Short ripples and small leaves", "Low-density horizontal flow", "Keep ripple lines lighter than leaf contours.", S_CO),
            P("IW08", "Tiny Lily", "Coordinate", "Small simplified lilies", "Open flower scatter", "Reduce petal layers at this scale.", S_CO),
            P("IW09", "Rippling Water", "Blender", "Very fine shallow curved lines", "Low-contrast continuous water rhythm", "Check that curves meet smoothly across the repeat.", S_BL),
            P("IW10", "Lily & Dot", "Micro", "Tiny lilies and small dots", "Sparse micro scatter", "Use a delicate dot density and broad background space.", S_MI),
        ]),
]

BY_CODE = {c["code"]: c for c in COLLECTIONS}
SEASONAL = [c for c in COLLECTIONS if c["family"] == "Botanical & Seasonal"]
SACRED = [c for c in COLLECTIONS if c["family"] == "Sacred Seasons"]

# O Holy Night: song mappings and the original twelve-print detail (earlier design branch, Appendix C)
OH_SONGS = {
    "OH01": "O Holy Night", "OH02": "Hark! The Herald Angels Sing", "OH03": "Joy to the World",
    "OH04": "O Little Town of Bethlehem", "OH05": "Silent Night", "OH06": "We Three Kings",
    "OH07": "O Come, All Ye Faithful", "OH08": "I Heard the Bells on Christmas Day",
    "OH09": "The First Noel", "OH10": "Angels We Have Heard on High",
    "OH11": "It Came Upon the Midnight Clear", "OH12": "Away in a Manger",
}

def D(theme, orig, alt, pair, inspiration, steps, pitfalls, products):
    return dict(theme=theme, orig=orig, alt=alt, pair=pair, inspiration=inspiration, steps=steps, pitfalls=pitfalls, products=products)

OH_DETAIL = {
    "OH01": D("Wonder, hope, and a world transformed by the birth of Christ. The central image is light entering darkness; the nativity is intimate rather than crowded.",
              "Midnight; Ivory; Gold; Candlelight", "Ivory ground with midnight outlines and antique-gold rays.", "Bethlehem Starlight + Manger Linen",
              "Look at the composition of devotional miniatures: a sheltering arch, grouped figures, and light arranged around a small focal point. Borrow the structure and restraint, then invent your own drawing.",
              ["Sketch the shelter and manger as two simple silhouettes.", "Fit the bowed figures around the child without merging their outlines.", "Add star rays and olive sprigs; test the grouping in a half-drop."],
              "A single large scene, detailed facial features, or a star competing with other celestial symbols.",
              "Statement gift wrap, fabric panels, stationery covers, keepsake boxes."),
    "OH02": D("A joyful announcement: angels proclaim the birth of Christ. Translate that proclamation into upward movement, opened wings, and the long sweep of a trumpet.",
              "Ivory; Gold; Cranberry; Oat", "Midnight ground with ivory gowns and gold instruments.", "Gloria Garland + Carol Ribbon",
              "Study the silhouette of engraved angels, the layered structure of bird feathers, and the flared bell of an old brass trumpet. Combine those observations into original figures.",
              ["Draw one standing and two gently floating gown silhouettes.", "Construct each wing from broad feather groups before adding fine lines.", "Place trumpet and ribbon diagonals across an offset repeat."],
              "Cartoon cherubs, crowded clouds, heavy halos, or lettering on the ribbons.",
              "Premium wrap, decorative textiles, journal covers, holiday cards."),
    "OH03": D("Creation joining in celebration. Let the song inspire an abundant botanical rhythm, with winter greenery opening outward like a chorus.",
              "Evergreen; Ivory; Cranberry; Gold", "Ivory ground with evergreen foliage and restrained cranberry berries.", "Bells of Peace + Manger Linen",
              "Use botanical engraving for leaf contours and vein structure, illuminated borders for flowing stems, and the open five-petal form of hellebores for calm ivory focal points.",
              ["Draw one holly branch, one fir tip, and two blossom angles.", "Build three bouquets with different proportions of flowers and leaves.", "Rotate and interlock the bouquets, checking the dark gaps."],
              "Bright cherry red, oversized berries, identical bouquets, or competing floral species.",
              "All-over fabric, table linens, wrap, decorative packaging."),
    "OH04": D("A quiet town holding an extraordinary hope. Express the contrast between sleeping streets and small, warm windows rather than making every building a landmark.",
              "Midnight; Ivory; Oat; Candlelight", "Oat ground with midnight outlines and ivory buildings.", "The Holy Night + Bethlehem Starlight",
              "Sketch masonry outlines, modest roof shapes, and the dark vertical silhouette of a cypress. Use vintage village illustrations for simplified grouping rather than literal architectural copying.",
              ["Draw five small houses with distinct but simple rooflines.", "Group houses around one tree and a few lighted windows.", "Offset the clusters and remove windows that make the scene too busy."],
              "Snow, European church spires, detailed streets, or a competing oversized moon.",
              "Wrapping paper, tea towels, fabric borders, card backgrounds."),
    "OH05": D("Stillness, tenderness, and sacred peace. A candle is an interpretive symbol of that quiet feeling; the print should slow the eye rather than announce itself.",
              "Ivory; Gold; Oat; Candlelight", "Deep evergreen ground with ivory candles and gold holders.", "The Holy Night + Gloria Garland",
              "Observe taper candles, worn brass candlesticks, olive-leaf shapes, and the spare linework of devotional stationery. Simplify each object before adding any ornamental detail.",
              ["Draw three candle heights with the same narrow proportions.", "Test flame and holder silhouettes at small print size.", "Space the candles generously and add only a few olive sprigs."],
              "Blurry glow effects, melting wax drama, elaborate candelabras, or dense decoration.",
              "Tissue, stationery liners, fabric, soft background wrap."),
    "OH06": D("A guided journey ending in an offering. Translate gold, frankincense, and myrrh into small ceremonial treasures rather than a literal procession of kings.",
              "Cranberry; Gold; Ivory; Oat", "Ivory ground with cranberry shadows and gold vessels.", "Heralds of Glory + Carol Ribbon",
              "Look at engraved metalwork, old apothecary vessels, small ceremonial boxes, and the controlled curl of incense. Use those forms as drawing prompts rather than historical reconstructions.",
              ["Sketch each vessel as a distinct closed silhouette.", "Add one shared border detail and restrained gold hatching.", "Build gift clusters with one guiding star and a light incense curve."],
              "Modern wrapped presents, excessive jewels, crowded crowns, or detailed royal portraits.",
              "Keepsake packaging, gift wrap, upholstery accents, stationery."),
    "OH07": D("An invitation to gather in reverence. An open garland arch becomes a welcoming threshold; a small lantern suggests the light toward which people travel.",
              "Evergreen; Gold; Ivory; Cranberry", "Ivory ground with evergreen arches and gold lanterns.", "Heaven and Nature + Manger Linen",
              "Study illuminated arch borders, slender lantern frames, natural garland drape, and the curve of an olive branch. Keep the architectural suggestion light and decorative.",
              ["Draw one simple arch with a gently uneven garland curve.", "Create two small lanterns that fit comfortably inside it.", "Offset the arches and vary only the center motif and ribbon detail."],
              "Heavy cathedral detail, thick wreaths, large bows, or crowded human processions.",
              "Decorative fabric, wrap, placemats, packaging side panels."),
    "OH08": D("Hope and peace heard through the ringing of bells. Give the repeat a gentle swaying rhythm, using paired bells and curved sprigs to suggest sound.",
              "Ivory; Gold; Cranberry; Evergreen", "Midnight ground with gold bells and ivory highlights.", "Heaven and Nature + Carol Ribbon",
              "Observe the silhouette of a historic church bell, its flared rim, and the way a narrow ribbon falls around a handle. Use fir needles as a delicate counterpoint.",
              ["Draw front and three-quarter views of one bell design.", "Add a narrow bow that leaves the bell shape readable.", "Alternate single and double bells, balancing the spaces between them."],
              "Round jingle-bell clip art, large ribbon bows, ringing motion lines, or shiny metal gradients.",
              "Gift wrap, fabric, ribbon-adjacent packaging, napkins."),
    "OH09": D("Guidance and discovery beneath the star. Reduce the story to a small, recognizable celestial mark that carries the collection's sacred identity quietly.",
              "Midnight; Gold; Ivory", "Ivory ground with small antique-gold stars.", "The Holy Night + Bethlehem at Dusk",
              "Study eight-point ornamental stars and the spare dot patterns of vintage endpapers. Draw a consistent family instead of mixing unrelated star silhouettes.",
              ["Draw a small star on a vertical and horizontal axis.", "Create a short-ray and an elongated-ray variation.", "Repeat at actual size, then remove stars until the field feels calm."],
              "Large halos, many different star shapes, constellations, or a dominant moon.",
              "Liners, binding fabric, tissue, small packaging, card backs."),
    "OH10": D("Praise flowing outward like a sustained chorus. Abstract the angel's wing into feather curves and soft scallops, carrying the celestial theme without more figures.",
              "Ivory; Gold; Oat", "Evergreen ground with ivory feather curves and gold ticks.", "Heralds of Glory + A Still Small Light",
              "Use the contour of a single feather, the flow of a wingtip, and small scalloped manuscript borders as starting forms. Favor shallow curves and light line weight.",
              ["Simplify one feather into a curved spine and two small strokes.", "Join several curves into a shallow scallop.", "Repeat in offset rows and open the spacing until the ivory dominates."],
              "Literal tiny angels, dense lace, intricate feather barbs, or high-contrast outlines.",
              "Liners, fabric coordinates, stationery borders, napkins."),
    "OH11": D("A peaceful message carried across the night. Suggest distant caroling through the rhythm of fine flowing lines rather than drawing a literal musical score.",
              "Cranberry; Gold; Ivory", "Ivory ground with fine cranberry and gold ribbon lines.", "Heralds of Glory + Gifts of the Magi",
              "Observe the shallow wave of a narrow ribbon laid flat and the fine stripes of old book endpapers. Translate that movement into controlled, original linework.",
              ["Draw one shallow wave and a closely spaced companion line.", "Repeat the pair at a steady narrow interval.", "Add sparse star pulses and test the join at both ends of the stripe."],
              "Dense musical notation, broad candy-cane stripes, large bows, or random line spacing.",
              "Binding, liners, borders, packaging trim, small fabric pieces."),
    "OH12": D("Humility, shelter, and tenderness. Translate the simplicity of the manger into a warm woven-looking texture that rests between the collection's more descriptive prints.",
              "Oat; Ivory", "Ivory ground with pale oat texture marks.", "The Holy Night + Heaven and Nature",
              "Study loose linen weave and a small straw bundle for crossing directions and irregular ends. Simplify them into a few repeatable marks with generous space.",
              ["Draw three short straw-like strokes and one open woven square.", "Alternate their direction inside a small repeat.", "Reduce contrast and view from a distance until the texture becomes quiet."],
              "Photographic fabric texture, distressed stains, heavy crosshatching, or obvious large straw bundles.",
              "Backing fabric, binding, tissue, packaging interiors, stationery."),
}


# Condensed O Holy Night briefs for the one-page lineup (full text lives in the Book 3 appendix)
OH_SHORT = {
    "OH01": ("Holy Family beside a manger; simple shelter arch; eight-pointed Star of Bethlehem; olive sprigs; narrow gold rays.", "Narrative half-drop of three slightly different nativity vignettes linked by olive branches; the night ground keeps each scene readable.", "The star and radiating arch are the strongest shapes; faces and folds reduced to a few marks so it still reads when printed small."),
    "OH02": ("Three robed angel poses; feathered wings; long herald trumpets; blank curling ribbons; small stars; light flourishes.", "Alternate left- and right-facing angels in a half-drop; trumpet diagonals and ribbon curves lead the eye; open cream pockets around the wings.", "The trumpet supplies the motion. An elegant storybook figure with believable wings and a quiet expression."),
    "OH03": ("Holly and berries; fir tips; olive sprigs; cream hellebore blossoms; tiny gold-colored starbursts.", "Rounded bouquets interlocked on diagonal paths, some rotated for a multidirectional field; varied sizes give movement.", "The abundant hero: full clusters with dark gaps preserved so it never becomes a solid floral mass."),
    "OH04": ("Simple stone houses; flat roofs and an occasional dome; arched windows; cypress and olive trees; tiny gold stars.", "Three village clusters in softly staggered horizontal bands with strips of night sky between them; the houses stay directional.", "Suggestive of Bethlehem in the manner of old European cards; a few lit windows carry the feeling."),
    "OH05": ("Slim candles; modest brass holders; three flame shapes; fine radiating arcs; olive sprigs; a few gold dots.", "Spacious tossed repeat with candles at different heights, kept nearly upright, sprigs scattered between the holders.", "A small gold flame and one or two crisp arcs; warmth comes from spacing and color, with most of the paper quiet."),
    "OH06": ("A lidded gold casket; an incense vessel with a curling wisp; a narrow myrrh jar; guiding stars; occasional olive sprigs.", "Three small gift clusters alternating in an offset repeat; vessels turned slightly; incense curves link the groups.", "Separate the gifts by silhouette (low casket, wide vessel, slender jar) and repeat a few shared ornament marks."),
    "OH07": ("Open olive-and-holly arches; delicate hanging lanterns; small candles; narrow ribbon tails; sparse gold leaf ticks.", "Gently offset arches alternating lantern and candle centers; the center of each arch left open; only a few ribbons per tile.", "Open arches rather than closed wreaths: a sense of arrival, and distinct from the botanical hero."),
    "OH08": ("Antique flared church bells with small clappers; narrow berry-red bows; fir sprigs; single and paired arrangements.", "Bell groups tossed at restrained alternating angles; some paired, some single; bows and fir placed so each silhouette stays clear.", "Old and ceremonial: a wide mouth, a small clapper and only a few engraved lines to suggest the metal."),
    "OH09": ("Eight-pointed stars in two sizes; a few cream pinpoints; one fine outline variation matching the hero.", "Open offset scatter with a steady rhythm; larger stars separated; small pinpoints break the grid without a dense sky.", "The stars stand for the Star of Bethlehem: eight points, a slightly longer vertical ray and generous spacing."),
    "OH10": ("A few curved feather strokes; shallow scallops; tiny paired olive leaves; delicate gold arcs.", "Feather curves linked into gently repeating scallop rows, alternate rows offset; connecting leaves kept tiny.", "Echo an angel wing abstractly; two or three strokes per feather survive at small size."),
    "OH11": ("Thin undulating ribbons; alternating gold and cream lines; rare tiny star pulses; a few radiating marks.", "Restrained lengthwise stripe with a shallow wave; star accents staggered across lines; check the flow at the tile edges.", "The line rhythm is the reference to song; keep it quieter than the bell and trumpet prints."),
    "OH12": ("Broken cream crosshatching; tiny straw-like strokes; open basketweave squares; alternating parchment and cream marks.", "A small geometric texture with slight variation in the marks; even density and very low contrast, almost a solid.", "Linen through drawn marks rather than a photographic surface: warmth and visual rest, no new narrative motif."),
}


def role_counts(col):
    counts = {r: 0 for r in ROLE_ORDER}
    for p in col["prints"]:
        counts[p["role"]] += 1
    return counts


def validate():
    assert len(PALETTE) == 23, len(PALETTE)
    assert len(set(n for n, _ in PALETTE)) == 23
    assert len(set(h for _, h in PALETTE)) == 23
    ids = [p["id"] for c in COLLECTIONS for p in c["prints"]]
    assert len(ids) == 122 and len(set(ids)) == 122, len(ids)
    names = [(c["code"], p["name"]) for c in COLLECTIONS for p in c["prints"]]
    assert len(set(names)) == 122
    for c in COLLECTIONS:
        rc = role_counts(c)
        if c["code"] == "OH":
            assert len(c["prints"]) == 12 and rc == {"Hero": 3, "Secondary": 5, "Coordinate": 4, "Blender": 0, "Micro": 0}, rc
        else:
            assert len(c["prints"]) == 10 and rc == {"Hero": 2, "Secondary": 2, "Coordinate": 4, "Blender": 1, "Micro": 1}, (c["code"], rc)
        for n in c["palette"]:
            assert n in HEX, n
        for i, p in enumerate(c["prints"]):
            assert p["id"] == "%s%02d" % (c["code"], i + 1), p["id"]
    assert sum(len(c["prints"]) for c in SEASONAL) == 60
    assert sum(len(c["prints"]) for c in SACRED) == 62
    for s in SUBSETS:
        for n in s[1]:
            assert n in HEX, n
    assert set(OH_SONGS) == set(OH_DETAIL) == set(p["id"] for p in BY_CODE["OH"]["prints"])
    return True


if __name__ == "__main__":
    validate()
    print("data OK: 23 colors, %d collections, 122 prints" % len(COLLECTIONS))
